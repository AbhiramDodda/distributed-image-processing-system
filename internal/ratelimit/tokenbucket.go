// Package ratelimit provides a per-tenant token-bucket limiter for the platform
// API gateway (Level 6). A token bucket -- rather than a fixed window -- is the
// right primitive here because it admits short bursts (a tenant firing 50 job
// submissions at once) while still capping the sustained rate, and it has no
// window-boundary spike where a client gets 2x the intended rate by straddling
// the reset.
//
// Refill is lazy: there is no background goroutine topping up buckets. Each call
// computes how many tokens accrued since the bucket was last touched. This keeps
// an idle limiter free of any running cost and makes behaviour a pure function
// of the injected clock, which is what the tests exploit.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// Limiter hands out tokens per key (typically a tenant id). Every key gets its
// own bucket with the same rate and burst; a spendthrift tenant cannot drain the
// allowance of a quiet one.
type Limiter struct {
	mu sync.Mutex
	rate float64 // tokens per second
	burst float64 // bucket capacity
	buckets map[string]*bucket
	now func() time.Time
}

type bucket struct {
	tokens float64
	last time.Time
}

// New returns a Limiter that refills at rate tokens/second and lets a key hold at
// most burst tokens. A non-positive rate or burst is clamped to a tiny positive
// value so the limiter never silently admits everything.
func New(rate float64, burst int) *Limiter {
	if rate <= 0 {
		rate = 1
	}
	b := float64(burst)
	if b <= 0 {
		b = 1
	}
	return &Limiter{
		rate: rate,
		burst: b,
		buckets: make(map[string]*bucket),
		now: time.Now,
	}
}

// Allow reports whether one token is available for key, consuming it if so.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN reports whether n tokens are available for key, consuming them all-or-
// nothing if so. A request for zero or fewer tokens is always allowed and costs
// nothing.
func (l *Limiter) AllowN(key string, n int) bool {
	if n <= 0 {
		return true
	}
	need := float64(n)

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = now
	}
	if b.tokens >= need {
		b.tokens -= need
		return true
	}
	return false
}

// Tokens returns the number of tokens currently available to key, after applying
// any pending refill. Intended for metrics and tests, not for admission (use
// Allow, which consumes atomically).
func (l *Limiter) Tokens(key string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		return l.burst
	}
	if elapsed := l.now().Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = l.now()
	}
	return b.tokens
}
