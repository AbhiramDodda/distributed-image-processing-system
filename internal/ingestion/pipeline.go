package ingestion

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metadata"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

type Progress struct {
	Processed int64
	Failed int64
	Bytes int64

	// Aggregate per-phase busy time across all workers (nanoseconds). Because the
	// workers run concurrently these sum to more than the wall-clock elapsed; the
	// value is the relative cost breakdown (local open+stat vs checksum read+hash
	// vs S3 upload vs DB insert), not a timeline. This is what answers "where does
	// ingest time actually go" without guessing.
	OpenNanos int64
	ChecksumNanos int64
	UploadNanos int64
	IndexNanos int64
}

// phaseTimes carries the per-file timing of the upload-side phases (open+stat,
// checksum, S3 upload) back to the worker loop, which folds it into the pipeline-wide
// atomic accumulators. The DB-insert phase is no longer here: inserts are batched by a
// single writer goroutine (see IngestDir), so that phase is timed there. Whatever
// phases completed before an error are still reported so a failing file still accounts
// for the work it did.
type phaseTimes struct {
	open int64
	checksum int64
	upload int64
}

// indexReq hands a successfully-uploaded object's record from an upload worker to the
// single batching writer goroutine, along with its size for the byte accounting.
type indexReq struct {
	rec metadata.DataRecord
	size int64
}

type Pipeline struct {
	store *storage.Client
	idx *metadata.Index
	workers int
	log *slog.Logger
	batchSize int
	multipartThreshold int64
	multipartChunkSize int64
	multipartConcurrency int
}

func New(store *storage.Client, idx *metadata.Index, workers int, log *slog.Logger) *Pipeline {
	return &Pipeline{
		store: store,
		idx: idx,
		workers: workers,
		log: log,
		batchSize: 500,
		multipartThreshold: 100 * 1024 * 1024,
		multipartChunkSize: 64 * 1024 * 1024,
		multipartConcurrency: 8,
	}
}

func (p *Pipeline) WithMultipart(threshold, chunkSize int64, concurrency int) {
	p.multipartThreshold = threshold
	p.multipartChunkSize = chunkSize
	p.multipartConcurrency = concurrency
}

// WithBatchSize sets how many records the writer goroutine groups into one SQLite
// transaction. Larger batches amortize the writer's lock+fsync over more rows; a
// value <= 0 is ignored so callers can pass an unset config field harmlessly.
func (p *Pipeline) WithBatchSize(n int) {
	if n > 0 {
		p.batchSize = n
	}
}

type fileJob struct {
	localPath string
	dataset string
	labels []string
}

func (p *Pipeline) IngestDir(ctx context.Context, localDir, dataset string, labels []string) (*Progress, error) {
	jobs := make(chan fileJob, p.workers*2)
	// Buffer the writer's inbox generously so upload workers never block handing
	// off a finished record while the writer is mid-commit.
	records := make(chan indexReq, p.workers*4)
	var wg sync.WaitGroup
	var processed, failed, bytes atomic.Int64
	var openNanos, checksumNanos, uploadNanos, indexNanos atomic.Int64

	// Upload workers: the parallel, I/O-heavy half — open, checksum, and push the
	// object to S3. On success they hand the record to the writer and move on; they
	// never touch SQLite, so there is no per-file write-lock contention between them.
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				rec, n, t, err := p.uploadFile(ctx, job)
				openNanos.Add(t.open)
				checksumNanos.Add(t.checksum)
				uploadNanos.Add(t.upload)
				if err != nil {
					p.log.Error("ingest file failed", "path", job.localPath, "err", err)
					failed.Add(1)
					continue
				}
				records <- indexReq{rec: rec, size: n}
			}
		}()
	}

	// Single writer: the whole reason this ingest stopped being 91% lock-wait. One
	// goroutine drains the records channel and commits them in batches, so SQLite's
	// single writer is contended by exactly one caller and fsyncs once per batch
	// instead of once per image. A batch that fails to commit counts its whole span
	// as failed rather than dropping rows silently.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		batch := make([]metadata.DataRecord, 0, p.batchSize)
		var batchBytes int64
		flush := func() {
			if len(batch) == 0 {
				return
			}
			ti := time.Now()
			err := p.idx.InsertBatch(ctx, batch)
			indexNanos.Add(time.Since(ti).Nanoseconds())
			if err != nil {
				p.log.Error("batch insert failed", "n", len(batch), "err", err)
				failed.Add(int64(len(batch)))
			} else {
				processed.Add(int64(len(batch)))
				bytes.Add(batchBytes)
			}
			batch = batch[:0]
			batchBytes = 0
		}
		for req := range records {
			batch = append(batch, req.rec)
			batchBytes += req.size
			if len(batch) >= p.batchSize {
				flush()
			}
		}
		flush()
	}()

	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !isImageFile(path) {
			return nil
		}
		select {
		case jobs <- fileJob{localPath: path, dataset: dataset, labels: labels}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	close(jobs)
	wg.Wait()       // all uploads finished, so no more records will be sent
	close(records)  // let the writer drain its inbox and flush the final batch
	<-writerDone

	return &Progress{
		Processed: processed.Load(),
		Failed: failed.Load(),
		Bytes: bytes.Load(),
		OpenNanos: openNanos.Load(),
		ChecksumNanos: checksumNanos.Load(),
		UploadNanos: uploadNanos.Load(),
		IndexNanos: indexNanos.Load(),
	}, err
}

// uploadFile does the parallelizable part of ingest — open, checksum, S3 upload —
// and returns the record to be indexed. It deliberately does NOT write to SQLite;
// the caller hands the record to the single batching writer.
func (p *Pipeline) uploadFile(ctx context.Context, job fileJob) (metadata.DataRecord, int64, phaseTimes, error) {
	var t phaseTimes

	t0 := time.Now()
	f, err := os.Open(job.localPath)
	if err != nil {
		return metadata.DataRecord{}, 0, t, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return metadata.DataRecord{}, 0, t, fmt.Errorf("stat: %w", err)
	}
	t.open = time.Since(t0).Nanoseconds()

	filename := filepath.Base(job.localPath)
	key := storage.ObjectKey(job.dataset, filename)

	tc := time.Now()
	checksum, err := sha256File(job.localPath)
	if err != nil {
		return metadata.DataRecord{}, 0, t, fmt.Errorf("checksum: %w", err)
	}
	t.checksum = time.Since(tc).Nanoseconds()

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return metadata.DataRecord{}, 0, t, err
	}

	tu := time.Now()
	if info.Size() >= p.multipartThreshold {
		if err := p.store.MultipartUpload(ctx, key, f, p.multipartChunkSize, p.multipartConcurrency); err != nil {
			return metadata.DataRecord{}, 0, t, fmt.Errorf("multipart upload: %w", err)
		}
	} else {
		if err := p.store.Put(ctx, key, f, mimeType(filename)); err != nil {
			return metadata.DataRecord{}, 0, t, fmt.Errorf("put: %w", err)
		}
	}
	t.upload = time.Since(tu).Nanoseconds()

	rec := metadata.DataRecord{
		ID: uuid.New().String(),
		Filename: filename,
		S3Key: key,
		Shard: storage.ShardKey(filename),
		Dataset: job.dataset,
		SizeBytes: info.Size(),
		Checksum: checksum,
		Labels: job.labels,
		Meta: map[string]string{"source_path": job.localPath},
		Tier: storage.TierHot,
		IndexedAt: time.Now(),
	}
	return rec, info.Size(), t, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func isImageFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".heic":
		return true
	}
	return false
}

func mimeType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".tiff", ".tif":
		return "image/tiff"
	default:
		return "application/octet-stream"
	}
}
