package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

func keyGetter() GetFunc {
	return func(ctx context.Context, key string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("data:" + key)), nil
	}
}

func collect(ch <-chan Item) []Item {
	var out []Item
	for it := range ch {
		out = append(out, it)
	}
	return out
}

func TestPrefetcher_ordered(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	p := NewPrefetcher(keyGetter(), 3, discardLog())
	got := collect(p.Stream(context.Background(), keys))

	if len(got) != len(keys) {
		t.Fatalf("got %d items, want %d", len(got), len(keys))
	}
	for i, it := range got {
		if it.Err != nil {
			t.Fatalf("item %d error: %v", i, it.Err)
		}
		if it.Key != keys[i] {
			t.Fatalf("item %d key = %q, want %q (order not preserved)", i, it.Key, keys[i])
		}
		if string(it.Data) != "data:"+keys[i] {
			t.Fatalf("item %d data = %q", i, it.Data)
		}
	}
}

func TestPrefetcher_boundedLookahead(t *testing.T) {
	const depth = 3
	var inflight, maxInflight int32
	release := make(chan struct{})

	get := func(ctx context.Context, key string) (io.ReadCloser, error) {
		n := atomic.AddInt32(&inflight, 1)
		for {
			m := atomic.LoadInt32(&maxInflight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInflight, m, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&inflight, -1)
		return io.NopCloser(strings.NewReader(key)), nil
	}

	p := NewPrefetcher(get, depth, discardLog())
	ch := p.Stream(context.Background(), []string{"a", "b", "c", "d", "e", "f"})

	// No one is consuming yet, so the producer fills exactly `depth` futures and
	// then blocks. Wait for the fetches to reach the barrier.
	waitFor(t, func() bool { return atomic.LoadInt32(&inflight) == depth })
	// Give any (incorrect) extra fetch a chance to start before asserting.
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&maxInflight); got > depth {
		t.Fatalf("look-ahead unbounded: %d concurrent fetches, want <= %d", got, depth)
	}

	close(release)
	if got := collect(ch); len(got) != 6 {
		t.Fatalf("got %d items after release, want 6", len(got))
	}
}

func TestPrefetcher_errorPropagatesAndContinues(t *testing.T) {
	get := func(ctx context.Context, key string) (io.ReadCloser, error) {
		if key == "bad" {
			return nil, fmt.Errorf("boom")
		}
		return io.NopCloser(strings.NewReader(key)), nil
	}
	p := NewPrefetcher(get, 2, discardLog())
	got := collect(p.Stream(context.Background(), []string{"a", "bad", "c"}))

	if len(got) != 3 {
		t.Fatalf("got %d items, want 3", len(got))
	}
	if got[0].Err != nil || got[2].Err != nil {
		t.Fatalf("good items reported errors: %+v", got)
	}
	if got[1].Err == nil {
		t.Fatal("expected error for key 'bad'")
	}
	if got[1].Key != "bad" {
		t.Fatalf("error item out of order: key=%q", got[1].Key)
	}
}

func TestPrefetcher_contextCancel(t *testing.T) {
	started := make(chan struct{}, 8)
	get := func(ctx context.Context, key string) (io.ReadCloser, error) {
		started <- struct{}{}
		<-ctx.Done() // a well-behaved fetch must return on cancellation
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPrefetcher(get, 2, discardLog())
	ch := p.Stream(ctx, []string{"a", "b", "c", "d", "e"})

	<-started // ensure at least one fetch is genuinely in progress
	cancel()

	// The stream must terminate (channel close) promptly, proving no goroutine
	// deadlocks on a cancelled context.
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate after cancellation")
	}
}

func TestPrefetcher_emptyKeys(t *testing.T) {
	p := NewPrefetcher(keyGetter(), 4, discardLog())
	if got := collect(p.Stream(context.Background(), nil)); len(got) != 0 {
		t.Fatalf("empty input yielded %d items", len(got))
	}
}

func TestNewPrefetcher_clampsDepth(t *testing.T) {
	p := NewPrefetcher(keyGetter(), 0, discardLog())
	if p.depth != 1 {
		t.Fatalf("depth = %d, want clamped to 1", p.depth)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
