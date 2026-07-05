package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeSource is an in-memory stand-in for storage.Client. It records how many
// times each key was actually fetched so tests can assert cache behaviour.
type fakeSource struct {
	mu sync.Mutex
	data map[string][]byte
	calls map[string]int
	block chan struct{} // if non-nil, every fetch waits on it (concurrency tests)
}

func newSource() *fakeSource {
	return &fakeSource{data: map[string][]byte{}, calls: map[string]int{}}
}

func (s *fakeSource) put(key, val string) { s.data[key] = []byte(val) }

func (s *fakeSource) fetch(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	s.calls[key]++
	b, ok := s.data[key]
	s.mu.Unlock()
	if !ok {
		return nil, 0, fmt.Errorf("no such object: %s", key)
	}
	return io.NopCloser(strings.NewReader(string(b))), int64(len(b)), nil
}

func (s *fakeSource) callCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[key]
}

func testCache(t *testing.T, maxBytes int64) *Cache {
	t.Helper()
	c, err := New(t.TempDir(), maxBytes, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func getString(t *testing.T, c *Cache, key string, fetch FetchFunc) string {
	t.Helper()
	rc, err := c.Get(context.Background(), key, fetch)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(b)
}

func TestCache_missThenHit(t *testing.T) {
	src := newSource()
	src.put("train/a3/cat.jpg", "meow")
	c := testCache(t, 1<<20)

	if got := getString(t, c, "train/a3/cat.jpg", src.fetch); got != "meow" {
		t.Fatalf("first get = %q, want meow", got)
	}
	if got := getString(t, c, "train/a3/cat.jpg", src.fetch); got != "meow" {
		t.Fatalf("second get = %q, want meow", got)
	}
	if n := src.callCount("train/a3/cat.jpg"); n != 1 {
		t.Fatalf("source fetched %d times, want 1 (second read should be a hit)", n)
	}
	st := c.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("stats = %+v, want hits=1 misses=1", st)
	}
}

func TestCache_fetchErrorNotAdmitted(t *testing.T) {
	src := newSource()
	c := testCache(t, 1<<20)
	if _, err := c.Get(context.Background(), "missing", src.fetch); err == nil {
		t.Fatal("expected error for missing object")
	}
	if st := c.Stats(); st.Objects != 0 || st.Bytes != 0 {
		t.Fatalf("failed fetch left state behind: %+v", st)
	}
	// A later successful fetch of a different key must still work.
	src.put("ok", "value")
	if got := getString(t, c, "ok", src.fetch); got != "value" {
		t.Fatalf("get after error = %q", got)
	}
}

func TestCache_lruEviction(t *testing.T) {
	src := newSource()
	for _, k := range []string{"a", "b", "c", "d"} {
		src.put(k, "0123456789") // 10 bytes each
	}
	c := testCache(t, 25) // holds at most 2 objects

	getString(t, c, "a", src.fetch)
	getString(t, c, "b", src.fetch)
	getString(t, c, "c", src.fetch) // admits c, evicts a (LRU)

	// Touch b so it becomes most-recently-used; d then evicts c, not b.
	getString(t, c, "b", src.fetch)
	getString(t, c, "d", src.fetch)

	// a and c were evicted -> refetching them hits the source again.
	getString(t, c, "b", src.fetch) // still cached -> no new fetch
	getString(t, c, "a", src.fetch) // evicted -> refetch

	if n := src.callCount("b"); n != 1 {
		t.Errorf("b fetched %d times, want 1 (should never be evicted)", n)
	}
	if n := src.callCount("a"); n != 2 {
		t.Errorf("a fetched %d times, want 2 (evicted then refetched)", n)
	}
	if n := src.callCount("c"); n != 1 {
		t.Errorf("c fetched %d times, want 1 (evicted by d, never refetched)", n)
	}
	if st := c.Stats(); st.Bytes > st.MaxBytes {
		t.Errorf("bytes %d exceed budget %d", st.Bytes, st.MaxBytes)
	}
}

func TestCache_oversizedObjectKept(t *testing.T) {
	src := newSource()
	src.put("big", strings.Repeat("x", 100))
	c := testCache(t, 25)
	if got := getString(t, c, "big", src.fetch); len(got) != 100 {
		t.Fatalf("oversized object len = %d, want 100", len(got))
	}
	if st := c.Stats(); st.Objects != 1 {
		t.Fatalf("oversized object should be retained, stats=%+v", st)
	}
}

func TestCache_singleFlight(t *testing.T) {
	src := newSource()
	src.put("hot", "shared")
	src.block = make(chan struct{})
	c := testCache(t, 1<<20)

	const readers = 24
	var wg sync.WaitGroup
	var okCount int64
	for range readers {
		wg.Go(func() {
			rc, err := c.Get(context.Background(), "hot", src.fetch)
			if err != nil {
				t.Errorf("concurrent Get: %v", err)
				return
			}
			b, _ := io.ReadAll(rc)
			rc.Close()
			if string(b) == "shared" {
				atomic.AddInt64(&okCount, 1)
			}
		})
	}
	close(src.block) // release all in-flight fetches at once
	wg.Wait()

	if okCount != readers {
		t.Fatalf("%d readers got correct data, want %d", okCount, readers)
	}
	if n := src.callCount("hot"); n != 1 {
		t.Fatalf("single-flight failed: source fetched %d times, want 1", n)
	}
}

func TestCache_persistsAcrossRestart(t *testing.T) {
	src := newSource()
	src.put("k1", "one")
	src.put("k2", "two")
	dir := t.TempDir()

	c1, err := New(dir, 1<<20, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New c1: %v", err)
	}
	getString(t, c1, "k1", src.fetch)
	getString(t, c1, "k2", src.fetch)

	// Simulate a coordinator/worker restart: brand new Cache over the same dir.
	c2, err := New(dir, 1<<20, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New c2: %v", err)
	}
	if st := c2.Stats(); st.Objects != 2 {
		t.Fatalf("adopted %d objects, want 2", st.Objects)
	}
	if got := getString(t, c2, "k1", src.fetch); got != "one" {
		t.Fatalf("k1 after restart = %q", got)
	}
	if st := c2.Stats(); st.Misses != 0 {
		t.Fatalf("adopted object should hit, got misses=%d", st.Misses)
	}
	if n := src.callCount("k1"); n != 1 {
		t.Fatalf("k1 fetched %d times total, want 1 (served from adopted cache)", n)
	}
}

func TestCache_adoptEvictsOverBudget(t *testing.T) {
	src := newSource()
	for _, k := range []string{"a", "b", "c", "d"} {
		src.put(k, "0123456789")
	}
	dir := t.TempDir()
	c1, _ := New(dir, 1<<20, slog.New(slog.DiscardHandler))
	for _, k := range []string{"a", "b", "c", "d"} {
		getString(t, c1, k, src.fetch)
	}
	// Restart with a budget that only fits two objects.
	c2, err := New(dir, 25, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New c2: %v", err)
	}
	st := c2.Stats()
	if st.Bytes > st.MaxBytes {
		t.Fatalf("adopt left %d bytes over budget %d", st.Bytes, st.MaxBytes)
	}
	if st.Objects > 2 {
		t.Fatalf("adopt kept %d objects, want <= 2", st.Objects)
	}
}

func TestNew_rejectsNonPositiveBudget(t *testing.T) {
	if _, err := New(t.TempDir(), 0, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected error for zero budget")
	}
}
