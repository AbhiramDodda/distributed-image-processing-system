package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MultipartUpload splits r into chunkSize parts and uploads them in parallel.
// Always aborts on failure to prevent orphaned partial uploads.
func (c *Client) MultipartUpload(ctx context.Context, key string, r io.Reader, chunkSize int64, concurrency int) error {
	create, err := c.s3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket),
		Key: aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := aws.ToString(create.UploadId)

	abort := func() {
		c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(c.bucket),
			Key: aws.String(key),
			UploadId: aws.String(uploadID),
		})
	}

	type part struct {
		num int32
		etag string
	}
	type work struct {
		num int32
		data []byte
	}

	workCh := make(chan work, concurrency)
	resultCh := make(chan part, concurrency)
	errCh := make(chan error, concurrency)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range workCh {
				out, err := c.s3.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(c.bucket),
					Key: aws.String(key),
					UploadId: aws.String(uploadID),
					PartNumber: aws.Int32(w.num),
					Body: newBytesReader(w.data),
				})
				if err != nil {
					errCh <- fmt.Errorf("upload part %d: %w", w.num, err)
					return
				}
				resultCh <- part{num: w.num, etag: aws.ToString(out.ETag)}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var partNum int32 = 1
	buf := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			workCh <- work{num: partNum, data: chunk}
			partNum++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			close(workCh)
			abort()
			return fmt.Errorf("read input: %w", err)
		}
	}
	close(workCh)

	select {
	case err := <-errCh:
		abort()
		return err
	default:
	}

	var parts []part
	for p := range resultCh {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].num < parts[j].num })

	completed := make([]s3types.CompletedPart, len(parts))
	for i, p := range parts {
		completed[i] = s3types.CompletedPart{
			PartNumber: aws.Int32(p.num),
			ETag: aws.String(p.etag),
		}
	}

	_, err = c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(c.bucket),
		Key: aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		abort()
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

type bytesReader struct {
	data []byte
	pos int
}

func newBytesReader(data []byte) *bytesReader { return &bytesReader{data: data} }

func (b *bytesReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
