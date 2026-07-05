// Package pipeline provides the high-throughput data-path primitives that feed
// compute: look-ahead prefetching of shard objects, streaming reduction, and
// (later) WAL-based checkpointing of coordinator state.
package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

// GetFunc retrieves one object's bytes. A cache.Cache is adapted to this by
// closing over its FetchFunc: func(ctx, k) { return c.Get(ctx, k, src.Get) }.
type GetFunc func(ctx context.Context, key string) (io.ReadCloser, error)

// Item is one prefetched object, delivered in submission order. Exactly one of
// Data or Err is set.
type Item struct {
	Key string
	Data []byte
	Err error
}

// Prefetcher streams objects with bounded look-ahead: while the consumer works
// on item N, up to depth subsequent objects are fetched concurrently. This
// overlaps S3/NVMe latency with GPU compute (double buffering when depth == 1).
type Prefetcher struct {
	get GetFunc
	depth int
	log *slog.Logger
}

// NewPrefetcher returns a Prefetcher that reads through get. depth is the number
// of objects fetched ahead of the consumer; values < 1 are clamped to 1 (plain
// double buffering).
func NewPrefetcher(get GetFunc, depth int, log *slog.Logger) *Prefetcher {
	if depth < 1 {
		depth = 1
	}
	return &Prefetcher{get: get, depth: depth, log: log}
}

// Stream fetches keys with bounded look-ahead and returns a channel that yields
// one Item per key, in the same order as keys. The channel is closed after the
// last key (or once ctx is cancelled). Concurrent fetches (and thus buffered
// memory) are capped at depth regardless of len(keys), so it is safe over a full
// shard listing.
//
// Ordering is preserved even though fetches run concurrently: each key is
// assigned a single-slot future, and the futures are drained in order.
func (p *Prefetcher) Stream(ctx context.Context, keys []string) <-chan Item {
	// sem caps the number of in-flight fetches at depth. A token is acquired
	// before a fetch starts and released when it finishes, so it bounds actual
	// concurrency independent of how far the ordered emitter has drained.
	sem := make(chan struct{}, p.depth)
	futures := make(chan chan Item, p.depth)

	go func() {
		defer close(futures)
		for _, key := range keys {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			f := make(chan Item, 1)
			select {
			case futures <- f:
			case <-ctx.Done():
				<-sem
				return
			}
			go func(key string, f chan<- Item) {
				defer func() { <-sem }()
				p.fetch(ctx, key, f)
			}(key, f)
		}
	}()

	out := make(chan Item)
	go func() {
		defer close(out)
		for f := range futures {
			var it Item
			select {
			case it = <-f:
			case <-ctx.Done():
				return
			}
			select {
			case out <- it:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// fetch resolves one future by reading the whole object into memory. The bytes
// are buffered so the reader (and its NVMe file handle) is released immediately,
// leaving the consumer holding only a plain slice. The future is buffered, so
// this never blocks even if the consumer has already stopped reading.
func (p *Prefetcher) fetch(ctx context.Context, key string, f chan<- Item) {
	rc, err := p.get(ctx, key)
	if err != nil {
		f <- Item{Key: key, Err: fmt.Errorf("prefetch %s: %w", key, err)}
		return
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		f <- Item{Key: key, Err: fmt.Errorf("prefetch read %s: %w", key, err)}
		return
	}
	f <- Item{Key: key, Data: data}
}
