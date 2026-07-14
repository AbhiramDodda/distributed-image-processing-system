// Package effect provides a reference exactly-once barrier for the platform's
// post-commit side effects. The scheduler delivers a task's side effect
// at-least-once, stamped with a deterministic idempotency key (see
// scheduler.SideEffectKey); this package is the receiver-side counterpart that
// collapses those redundant deliveries into a single observable action.
//
// The barrier is deliberately small and self-contained so the exactly-once
// property can be demonstrated end to end without an external sink. In production
// the same role is played by whatever the downstream already offers -- a unique
// constraint (INSERT ... ON CONFLICT DO NOTHING), a message broker's dedup window,
// an idempotency-key header on an HTTP API. The contract is identical: the action
// runs for a key at most once, atomically with recording that it ran.
package effect

import "sync"

// Ledger records which idempotency keys have had their action applied. Claim is
// the atomic test-and-set at the heart of the barrier: it returns true to exactly
// one caller per key, even under concurrent delivery, and false thereafter.
//
// Atomicity of Claim is what buys exactly-once: a real ledger claims and applies
// in one transaction (e.g. a unique-key INSERT that fails on a duplicate), so a
// crash cannot leave a key claimed-but-not-applied. A ledger that cannot make the
// action atomic with the claim degrades to at-least-once across a crash in that
// window -- still safe, because the key lets the next delivery retry.
type Ledger interface {
	Claim(key string) bool
}

// MemLedger is an in-memory Ledger: correct for a single process (the mutex makes
// Claim atomic) and the default for tests and single-node runs. It is not durable
// across a restart; back the barrier with a persistent ledger to survive one.
type MemLedger struct {
	mu sync.Mutex
	seen map[string]struct{}
}

// NewMemLedger returns an empty in-memory ledger.
func NewMemLedger() *MemLedger {
	return &MemLedger{seen: make(map[string]struct{})}
}

// Claim records key and reports whether this call is the first to do so.
func (m *MemLedger) Claim(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[key]; ok {
		return false
	}
	m.seen[key] = struct{}{}
	return true
}

// Len reports how many distinct keys have been applied -- for tests and metrics.
func (m *MemLedger) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.seen)
}
