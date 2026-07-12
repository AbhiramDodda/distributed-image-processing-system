package diag

import (
	"sync"
	"testing"
	"time"
)

// reset returns the package to a clean, enabled state for a test and disables it
// again on cleanup, so global diag state never leaks between tests.
func reset(t *testing.T) {
	t.Helper()
	resetViolations()
	resetGraph()
	Enable(nil, Config{WaitWarn: time.Hour, HoldWarn: time.Hour}) // silence timing warns
	t.Cleanup(func() {
		Disable()
		resetViolations()
		resetGraph()
	})
}

// A failing assertion records exactly one violation with its detail; a passing
// one records nothing.
func TestAssert_recordsViolation(t *testing.T) {
	reset(t)

	Assert(1 <= 2, "should not fire", "a", 1, "b", 2)
	if got := ViolationCount(); got != 0 {
		t.Fatalf("passing assert recorded %d violations, want 0", got)
	}

	Assert(5 <= 3, "frontier past granted", "frontier", 5, "granted", 3)
	if got := ViolationCount(); got != 1 {
		t.Fatalf("violation count = %d, want 1", got)
	}
	v := RecentViolations()
	if len(v) != 1 || v[0].Msg != "frontier past granted" {
		t.Fatalf("recent violations = %+v", v)
	}
	if v[0].Detail != "frontier=5 granted=3" {
		t.Errorf("detail = %q, want %q", v[0].Detail, "frontier=5 granted=3")
	}
	if v[0].Stack == "" {
		t.Error("violation has no stack trace")
	}
}

// When diagnostics are off, Assert is inert.
func TestAssert_noopWhenDisabled(t *testing.T) {
	resetViolations()
	Disable()
	Assert(false, "must be ignored while disabled")
	if got := ViolationCount(); got != 0 {
		t.Fatalf("violation recorded while disabled: %d", got)
	}
}

// The violation ring retains only the most recent maxViolations, newest last,
// while the total count keeps climbing.
func TestViolations_ringBounded(t *testing.T) {
	reset(t)
	const n = maxViolations + 50
	for i := 0; i < n; i++ {
		Assertf(false, "v", "i=%d", i)
	}
	if got := ViolationCount(); got != int64(n) {
		t.Fatalf("total count = %d, want %d", got, n)
	}
	got := RecentViolations()
	if len(got) != maxViolations {
		t.Fatalf("retained = %d, want %d", len(got), maxViolations)
	}
	// Newest last: the final entry must be the last one recorded.
	if want := "i=" + itoa(n-1); got[len(got)-1].Detail != want {
		t.Errorf("newest retained = %q, want %q", got[len(got)-1].Detail, want)
	}
}

// A single instrumented lock records its acquisitions and holder handoff.
func TestMutex_recordsAcquisitions(t *testing.T) {
	reset(t)
	var m Mutex
	m.SetName("test.mu")
	for i := 0; i < 5; i++ {
		m.Lock()
		m.Unlock()
	}
	stats := LockStats()
	var found *LockStat
	for i := range stats {
		if stats[i].Name == "test.mu" {
			found = &stats[i]
		}
	}
	if found == nil {
		t.Fatal("test.mu not in LockStats")
	}
	if found.Acquisitions != 5 {
		t.Errorf("acquisitions = %d, want 5", found.Acquisitions)
	}
}

// Taking two locks in opposite orders from two goroutines is a latent deadlock;
// the order graph must flag it as an inconsistent-order pair — without the code
// ever actually deadlocking.
func TestLockOrder_detectsInconsistentOrder(t *testing.T) {
	reset(t)
	var a, b Mutex
	a.SetName("A")
	b.SetName("B")

	// Goroutine 1: A then B.
	a.Lock()
	b.Lock()
	b.Unlock()
	a.Unlock()

	// This goroutine: B then A — the reverse order.
	b.Lock()
	a.Lock()
	a.Unlock()
	b.Unlock()

	if got := CycleCount(); got != 1 {
		t.Fatalf("cycle count = %d, want 1", got)
	}
	warns := LockOrderWarnings()
	if len(warns) != 1 {
		t.Fatalf("warnings = %+v, want exactly one", warns)
	}
	// The pair is {A,B} regardless of which side is reported first.
	if !(warns[0].A == "A" && warns[0].B == "B") && !(warns[0].A == "B" && warns[0].B == "A") {
		t.Errorf("warning pair = %+v, want {A,B}", warns[0])
	}
}

// A consistent lock order (always A before B) is never flagged.
func TestLockOrder_consistentOrderIsClean(t *testing.T) {
	reset(t)
	var a, b Mutex
	a.SetName("A")
	b.SetName("B")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Lock()
			b.Lock()
			b.Unlock()
			a.Unlock()
		}()
	}
	wg.Wait()

	if got := CycleCount(); got != 0 {
		t.Fatalf("consistent order flagged a cycle: count = %d", got)
	}
}

// The RWMutex read path is instrumented and concurrent readers coexist without
// corrupting the held-lock bookkeeping (each reader keyed by its own goid).
func TestRWMutex_concurrentReaders(t *testing.T) {
	reset(t)
	var rw RWMutex
	rw.SetName("rw")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rw.RLock()
			rw.RUnlock()
		}()
	}
	wg.Wait()

	rw.Lock()
	rw.Unlock()

	stats := LockStats()
	var found bool
	for _, s := range stats {
		if s.Name == "rw" {
			found = true
			if s.Acquisitions != 17 { // 16 readers + 1 writer
				t.Errorf("rw acquisitions = %d, want 17", s.Acquisitions)
			}
		}
	}
	if !found {
		t.Error("rw not registered")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
