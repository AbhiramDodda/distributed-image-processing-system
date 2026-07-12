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
}

type Pipeline struct {
	store *storage.Client
	idx *metadata.Index
	workers int
	log *slog.Logger
	multipartThreshold int64
	multipartChunkSize int64
	multipartConcurrency int
}

func New(store *storage.Client, idx *metadata.Index, workers int, log *slog.Logger) *Pipeline {
	return &Pipeline{
		store:                store,
		idx:                  idx,
		workers:              workers,
		log:                  log,
		multipartThreshold:   100 * 1024 * 1024,
		multipartChunkSize:   64 * 1024 * 1024,
		multipartConcurrency: 8,
	}
}

func (p *Pipeline) WithMultipart(threshold, chunkSize int64, concurrency int) {
	p.multipartThreshold = threshold
	p.multipartChunkSize = chunkSize
	p.multipartConcurrency = concurrency
}

type fileJob struct {
	localPath string
	dataset string
	labels []string
}

func (p *Pipeline) IngestDir(ctx context.Context, localDir, dataset string, labels []string) (*Progress, error) {
	jobs := make(chan fileJob, p.workers*2)
	var wg sync.WaitGroup
	var processed, failed, bytes atomic.Int64

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				n, err := p.ingestFile(ctx, job)
				if err != nil {
					p.log.Error("ingest file failed", "path", job.localPath, "err", err)
					failed.Add(1)
					continue
				}
				processed.Add(1)
				bytes.Add(n)
			}
		}()
	}

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
	wg.Wait()

	return &Progress{
		Processed: processed.Load(),
		Failed:    failed.Load(),
		Bytes:     bytes.Load(),
	}, err
}

func (p *Pipeline) ingestFile(ctx context.Context, job fileJob) (int64, error) {
	f, err := os.Open(job.localPath)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat: %w", err)
	}

	filename := filepath.Base(job.localPath)
	key := storage.ObjectKey(job.dataset, filename)

	checksum, err := sha256File(job.localPath)
	if err != nil {
		return 0, fmt.Errorf("checksum: %w", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	if info.Size() >= p.multipartThreshold {
		if err := p.store.MultipartUpload(ctx, key, f, p.multipartChunkSize, p.multipartConcurrency); err != nil {
			return 0, fmt.Errorf("multipart upload: %w", err)
		}
	} else {
		if err := p.store.Put(ctx, key, f, mimeType(filename)); err != nil {
			return 0, fmt.Errorf("put: %w", err)
		}
	}

	rec := metadata.DataRecord{
		ID:        uuid.New().String(),
		Filename:  filename,
		S3Key:     key,
		Shard:     storage.ShardKey(filename),
		Dataset:   job.dataset,
		SizeBytes: info.Size(),
		Checksum:  checksum,
		Labels:    job.labels,
		Meta:      map[string]string{"source_path": job.localPath},
		Tier:      storage.TierHot,
		IndexedAt: time.Now(),
	}

	if err := p.idx.Insert(ctx, rec); err != nil {
		return 0, fmt.Errorf("index: %w", err)
	}

	return info.Size(), nil
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
