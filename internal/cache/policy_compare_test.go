package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"testing"
)

// runWorkload replays the same access stream through a fresh cache under the
// given policy and returns the resulting Stats. The budget holds ~20% of the
// working set so eviction is the dominant effect.
func runWorkload(t *testing.T, policy EvictionPolicy, keys []string, stream []int) Stats {
	t.Helper()
	const objSize = 16
	src := newSource()
	for _, k := range keys {
		src.put(k, "0123456789abcdef") // objSize bytes each
	}
	budget := int64(len(keys)) * objSize / 5
	c, err := New(t.TempDir(), budget, slog.New(slog.DiscardHandler), WithPolicy(policy))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, idx := range stream {
		rc, err := c.Get(context.Background(), keys[idx], src.fetch)
		if err != nil {
			t.Fatalf("Get(%s): %v", keys[idx], err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
	return c.Stats()
}

// zipfStream builds a reproducible Zipfian access pattern over nKeys objects --
// a realistic model of shard popularity, where a few hot objects dominate reads.
func zipfStream(nKeys, accesses int, s float64) []int {
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, s, 1, uint64(nKeys-1))
	stream := make([]int, accesses)
	for i := range stream {
		stream[i] = int(zipf.Uint64())
	}
	return stream
}

// TestPolicy_compareHitRates is the experiment harness: it replays one Zipfian
// stream through LRU and through Clock and logs both hit rates so the difference
// is visible with `go test -run CompareHitRates -v ./internal/cache`. It asserts
// only invariants that must hold for either policy (full accounting, within
// budget); the hit-rate delta is reported, not gated, since it is workload
// dependent and the whole point is to observe it.
func TestPolicy_compareHitRates(t *testing.T) {
	const (
		nKeys = 200
		accesses = 8000
	)
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("shard/%04d", i)
	}
	stream := zipfStream(nKeys, accesses, 1.2)

	lru := runWorkload(t, NewLRU(), keys, stream)
	clock := runWorkload(t, NewClock(), keys, stream)

	hitRate := func(s Stats) float64 {
		total := s.Hits + s.Misses
		if total == 0 {
			return 0
		}
		return float64(s.Hits) / float64(total) * 100
	}
	t.Logf("LRU:   hits=%-5d misses=%-5d hitrate=%.1f%% bytes=%d/%d",
		lru.Hits, lru.Misses, hitRate(lru), lru.Bytes, lru.MaxBytes)
	t.Logf("Clock: hits=%-5d misses=%-5d hitrate=%.1f%% bytes=%d/%d",
		clock.Hits, clock.Misses, hitRate(clock), clock.Bytes, clock.MaxBytes)
	t.Logf("delta (Clock - LRU): %+.1f pp", hitRate(clock)-hitRate(lru))

	for name, s := range map[string]Stats{"lru": lru, "clock": clock} {
		if s.Hits+s.Misses != accesses {
			t.Fatalf("%s: accounting off, hits+misses=%d want %d", name, s.Hits+s.Misses, accesses)
		}
		if s.Bytes > s.MaxBytes {
			t.Fatalf("%s: over budget, bytes=%d max=%d", name, s.Bytes, s.MaxBytes)
		}
	}
}
