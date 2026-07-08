package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests advance time deterministically instead of sleeping.
type fakeClock struct {
	mu sync.Mutex
	t time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestLimiter(rate float64, burst int) (*Limiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := New(rate, burst)
	l.now = clk.now
	return l, clk
}

func TestLimiter_burstThenBlocks(t *testing.T) {
	l, _ := newTestLimiter(1, 5)
	for i := range 5 {
		if !l.Allow("t") {
			t.Fatalf("token %d within burst was denied", i)
		}
	}
	if l.Allow("t") {
		t.Fatal("6th token past burst should be denied")
	}
}

func TestLimiter_refillsOverTime(t *testing.T) {
	l, clk := newTestLimiter(2, 2) // 2 tokens/sec, burst 2
	for i := range 2 {
		if !l.Allow("t") {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if l.Allow("t") {
		t.Fatal("bucket should be empty")
	}
	clk.advance(500 * time.Millisecond) // 0.5s * 2/s = 1 token
	if !l.Allow("t") {
		t.Fatal("one token should have refilled")
	}
	if l.Allow("t") {
		t.Fatal("only one token should have refilled")
	}
}

func TestLimiter_refillCapsAtBurst(t *testing.T) {
	l, clk := newTestLimiter(10, 3)
	clk.advance(time.Hour) // would accrue 36000 tokens if uncapped
	if got := l.Tokens("t"); got != 3 {
		t.Fatalf("tokens = %.1f, want capped at burst 3", got)
	}
}

func TestLimiter_keysAreIsolated(t *testing.T) {
	l, _ := newTestLimiter(1, 1)
	if !l.Allow("a") {
		t.Fatal("a should get its token")
	}
	if !l.Allow("b") {
		t.Fatal("b has its own bucket and should get a token")
	}
	if l.Allow("a") {
		t.Fatal("a's bucket is drained")
	}
}

func TestLimiter_allowN(t *testing.T) {
	l, _ := newTestLimiter(1, 10)
	if !l.AllowN("t", 7) {
		t.Fatal("AllowN(7) within burst should succeed")
	}
	if l.AllowN("t", 4) {
		t.Fatal("AllowN(4) with only 3 left should be all-or-nothing denied")
	}
	if !l.AllowN("t", 3) {
		t.Fatal("AllowN(3) with exactly 3 left should succeed")
	}
	if !l.AllowN("t", 0) {
		t.Fatal("AllowN(0) is free and always allowed")
	}
}

// With a frozen clock (no refill) the number of granted tokens under concurrent
// access must be exactly the burst -- never more, proving the consume is atomic.
func TestLimiter_concurrentGrantsExactlyBurst(t *testing.T) {
	l, _ := newTestLimiter(1, 100)
	const goroutines = 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for range goroutines {
		wg.Go(func() {
			if l.Allow("t") {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if granted != 100 {
		t.Fatalf("granted %d tokens under contention, want exactly burst 100", granted)
	}
}

func TestLimiter_clampsNonPositiveConfig(t *testing.T) {
	l := New(0, 0)
	if !l.Allow("t") {
		t.Fatal("clamped limiter should still admit at least one token")
	}
}
