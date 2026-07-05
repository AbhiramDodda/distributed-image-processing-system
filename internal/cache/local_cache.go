// Package cache provides an NVMe-backed, size-bounded LRU cache for S3 objects.
//
// The compute path in Levels 3-4 places jobs on nodes that already hold the
// target shard on local NVMe. This cache is that local hold: the first read of
// an object pays the S3 round-trip, every subsequent read on the same node is
// served from disk. Capacity is bounded in bytes; the least-recently-used
// objects are evicted to stay under the limit.
package cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// FetchFunc retrieves an object on a cache miss. It mirrors storage.Client.Get,
// so *storage.Client.Get can be adapted to it directly.
type FetchFunc func(ctx context.Context, key string) (io.ReadCloser, int64, error)

// entry is one cached object. id is the hashed object key (also its on-disk
// filename); path is the full on-disk location; size is its byte count, tracked
// so eviction can bound total disk usage without re-stat'ing.
type entry struct {
	id string
	path string
	size int64
}

// call deduplicates concurrent misses for the same key so a stampede of readers
// triggers exactly one fetch (single-flight).
type call struct {
	wg sync.WaitGroup
	path string
	err error
}

type Cache struct {
	mu sync.Mutex
	root string
	maxBytes int64
	curBytes int64
	ll *list.List // front = most recently used, back = eviction candidate
	entries map[string]*list.Element
	inflight map[string]*call
	hits int64
	misses int64
	log *slog.Logger
}

// New opens (or creates) a cache rooted at dir with the given byte budget.
// Any objects already present under dir from a previous run are adopted, so the
// cache survives a process restart. maxBytes must be positive.
func New(dir string, maxBytes int64, log *slog.Logger) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("cache: maxBytes must be positive, got %d", maxBytes)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create root %s: %w", dir, err)
	}
	c := &Cache{
		root:     dir,
		maxBytes: maxBytes,
		ll:       list.New(),
		entries:  make(map[string]*list.Element),
		inflight: make(map[string]*call),
		log:      log,
	}
	if err := c.adopt(); err != nil {
		return nil, fmt.Errorf("cache: adopt existing objects: %w", err)
	}
	return c, nil
}

// Get returns a reader for the object identified by key. On a hit the object is
// served from NVMe and promoted to most-recently-used. On a miss fetch is
// invoked exactly once (even under concurrent callers), its bytes are persisted,
// and the object is admitted to the cache before the reader is returned.
//
// The caller owns the returned reader and must Close it.
func (c *Cache) Get(ctx context.Context, key string, fetch FetchFunc) (io.ReadCloser, error) {
	id := idFor(key)
	c.mu.Lock()
	if el, ok := c.entries[id]; ok {
		c.ll.MoveToFront(el)
		path := el.Value.(*entry).path
		c.hits++
		c.mu.Unlock()
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("cache: open hit %s: %w", key, err)
		}
		return f, nil
	}
	if cl, ok := c.inflight[id]; ok {
		c.mu.Unlock()
		cl.wg.Wait()
		if cl.err != nil {
			return nil, cl.err
		}
		f, err := os.Open(cl.path)
		if err != nil {
			return nil, fmt.Errorf("cache: open shared fetch %s: %w", key, err)
		}
		return f, nil
	}
	cl := &call{}
	cl.wg.Add(1)
	c.inflight[id] = cl
	c.misses++
	c.mu.Unlock()

	path, size, err := c.store(ctx, id, key, fetch)
	cl.path, cl.err = path, err
	cl.wg.Done()

	c.mu.Lock()
	delete(c.inflight, id)
	if err == nil {
		c.admit(id, path, size)
	}
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cache: open fetched %s: %w", key, err)
	}
	return f, nil
}

// store fetches the object and writes it to its final NVMe path via a temp file
// and atomic rename, so a crash mid-write never leaves a partial object visible.
// id is the hashed key (final filename); key is passed through to fetch.
func (c *Cache) store(ctx context.Context, id, key string, fetch FetchFunc) (string, int64, error) {
	rc, _, err := fetch(ctx, key)
	if err != nil {
		return "", 0, fmt.Errorf("cache: fetch %s: %w", key, err)
	}
	defer rc.Close()

	final := filepath.Join(c.root, id)
	tmp, err := os.CreateTemp(c.root, ".tmp-*")
	if err != nil {
		return "", 0, fmt.Errorf("cache: temp file for %s: %w", key, err)
	}
	n, err := io.Copy(tmp, rc)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", 0, fmt.Errorf("cache: write %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", 0, fmt.Errorf("cache: close temp for %s: %w", key, err)
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		os.Remove(tmp.Name())
		return "", 0, fmt.Errorf("cache: commit %s: %w", key, err)
	}
	return final, n, nil
}

// idFor maps an object key to a stable, filesystem-safe filename. The key is
// hashed so arbitrary S3 keys (slashes, unicode) become a flat hex filename;
// the id doubles as the in-memory index key so a restart can rebuild the index
// from filenames alone (see adopt).
func idFor(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Stats reports a point-in-time snapshot of cache occupancy and hit rate.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Objects: c.ll.Len(),
		Bytes: c.curBytes,
		MaxBytes: c.maxBytes,
		Hits: c.hits,
		Misses: c.misses,
	}
}

type Stats struct {
	Objects int `json:"objects"`
	Bytes int64 `json:"bytes"`
	MaxBytes int64 `json:"max_bytes"`
	Hits int64 `json:"hits"`
	Misses int64 `json:"misses"`
}
