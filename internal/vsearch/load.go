package vsearch

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// ObjectGetter is the slice of storage.Client that LoadIndex needs to pull a
// Parquet corpus from object storage. storage.Client satisfies it.
type ObjectGetter interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

// LoadIndex reads an embeddings Parquet file and builds a searchable Index. ref is
// either a local filesystem path or, when store is non-nil and ref does not resolve
// to a local file, an object key fetched from storage. Passing an "s3://key" ref
// forces the object-storage path.
func LoadIndex(ctx context.Context, store ObjectGetter, ref string) (*Index, error) {
	data, err := loadBytes(ctx, store, ref)
	if err != nil {
		return nil, err
	}
	dim, rows, err := ReadEmbeddingsParquet(data)
	if err != nil {
		return nil, err
	}
	return NewIndex(dim, rows)
}

func loadBytes(ctx context.Context, store ObjectGetter, ref string) ([]byte, error) {
	key := strings.TrimPrefix(ref, "s3://")
	forceObject := key != ref // had an s3:// scheme

	if !forceObject {
		if data, err := os.ReadFile(ref); err == nil {
			return data, nil
		} else if store == nil {
			return nil, fmt.Errorf("vsearch: read index %q: %w", ref, err)
		}
	}
	if store == nil {
		return nil, fmt.Errorf("vsearch: index %q needs object storage but none configured", ref)
	}
	rc, _, err := store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("vsearch: get index %q from storage: %w", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("vsearch: read index %q from storage: %w", key, err)
	}
	return data, nil
}
