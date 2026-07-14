package effect

import (
	"sync"
	"sync/atomic"
	"testing"
)

// The first Claim of a key wins; every later Claim of the same key loses. That is
// the exactly-once barrier a key-deduping sink is built on.
func TestMemLedger_claimOncePerKey(t *testing.T) {
	l := NewMemLedger()
	if !l.Claim("k1") {
		t.Fatal("first claim of k1 should win")
	}
	if l.Claim("k1") {
		t.Fatal("second claim of k1 should lose")
	}
	if !l.Claim("k2") {
		t.Fatal("first claim of a distinct key should win")
	}
	if l.Len() != 2 {
		t.Fatalf("ledger holds %d keys, want 2", l.Len())
	}
}

// Under concurrent delivery of the same key, Claim hands the win to exactly one
// caller -- the property that makes an at-least-once fan-in land exactly once.
func TestMemLedger_concurrentClaimSingleWinner(t *testing.T) {
	l := NewMemLedger()
	const goroutines = 64
	var wins int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range goroutines {
		wg.Go(func() {
			<-start // line everyone up to maximise the race
			if l.Claim("hot-key") {
				atomic.AddInt64(&wins, 1)
			}
		})
	}
	close(start)
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d goroutines won the claim, want exactly 1", wins)
	}
}
