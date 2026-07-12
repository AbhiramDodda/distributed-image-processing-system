package diag

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Mutex is a drop-in sync.Mutex that, when diagnostics are enabled, times how
// long callers wait for and hold it, tracks the current holder, and feeds the
// global lock-order graph used to predict deadlocks. When diagnostics are off it
// is a thin pass-through: one atomic load and the real Lock/Unlock.
//
// Give it a name with SetName so its stats and any lock-order warning are
// legible; an unnamed lock is reported as "?".
type Mutex struct {
	mu     sync.Mutex
	name   string
	st     atomic.Pointer[lockStat]
	holder atomic.Int64 // goid of current holder, 0 = free
	acqAt  atomic.Int64 // unixnano when the current holder acquired it
}

// RWMutex is the read/write analogue of Mutex. The write path (Lock/Unlock) is
// fully instrumented (wait, hold, holder, order). The read path (RLock/RUnlock)
// records wait and hold and participates in order tracking, but many readers may
// hold it at once, so it reports no single "holder".
type RWMutex struct {
	mu       sync.RWMutex
	name     string
	st       atomic.Pointer[lockStat]
	wHolder  atomic.Int64
	wAcqAt   atomic.Int64
	rHoldAt  sync.Map // goid -> unixnano acquire time, for per-reader hold timing
}

// SetName labels the lock. Call once before first use (e.g. in a constructor).
func (m *Mutex) SetName(n string)   { m.name = n }
func (m *RWMutex) SetName(n string) { m.name = n }

func (m *Mutex) Lock() {
	if !on.Load() {
		m.mu.Lock()
		return
	}
	st := statFor(&m.st, m.name)
	g := goid()
	enterAcquire(g, st.name)
	start := clock()
	m.mu.Lock()
	waited := clock().Sub(start)
	markHeld(g, st.name)
	m.holder.Store(g)
	m.acqAt.Store(clock().UnixNano())
	st.recordWait(waited)
}

func (m *Mutex) Unlock() {
	if !on.Load() {
		m.mu.Unlock()
		return
	}
	st := statFor(&m.st, m.name)
	held := time.Duration(clock().UnixNano() - m.acqAt.Load())
	g := m.holder.Load()
	m.holder.Store(0)
	m.mu.Unlock()
	markReleased(g, st.name)
	st.recordHold(held)
}

func (m *RWMutex) Lock() {
	if !on.Load() {
		m.mu.Lock()
		return
	}
	st := statFor(&m.st, m.name)
	g := goid()
	enterAcquire(g, st.name)
	start := clock()
	m.mu.Lock()
	waited := clock().Sub(start)
	markHeld(g, st.name)
	m.wHolder.Store(g)
	m.wAcqAt.Store(clock().UnixNano())
	st.recordWait(waited)
}

func (m *RWMutex) Unlock() {
	if !on.Load() {
		m.mu.Unlock()
		return
	}
	st := statFor(&m.st, m.name)
	held := time.Duration(clock().UnixNano() - m.wAcqAt.Load())
	g := m.wHolder.Load()
	m.wHolder.Store(0)
	m.mu.Unlock()
	markReleased(g, st.name)
	st.recordHold(held)
}

func (m *RWMutex) RLock() {
	if !on.Load() {
		m.mu.RLock()
		return
	}
	st := statFor(&m.st, m.name)
	g := goid()
	enterAcquire(g, st.name)
	start := clock()
	m.mu.RLock()
	waited := clock().Sub(start)
	markHeld(g, st.name)
	m.rHoldAt.Store(g, clock().UnixNano())
	st.recordWait(waited)
}

func (m *RWMutex) RUnlock() {
	if !on.Load() {
		m.mu.RUnlock()
		return
	}
	st := statFor(&m.st, m.name)
	g := goid()
	var held time.Duration
	if v, ok := m.rHoldAt.LoadAndDelete(g); ok {
		held = time.Duration(clock().UnixNano() - v.(int64))
	}
	m.mu.RUnlock()
	markReleased(g, st.name)
	st.recordHold(held)
}

// ---- per-lock statistics -------------------------------------------------

type lockStat struct {
	name         string
	acquisitions atomic.Int64
	waitTotal    atomic.Int64 // nanoseconds
	holdTotal    atomic.Int64 // nanoseconds
	maxWait      atomic.Int64
	maxHold      atomic.Int64
	contended    atomic.Int64 // acquisitions whose wait exceeded WaitWarn
	longHolds    atomic.Int64 // holds exceeding HoldWarn
}

// registry holds every named lock's stats so the endpoint can enumerate them.
var (
	registryMu sync.Mutex
	registry   = map[string]*lockStat{}
)

// statFor returns the lock's stat, lazily creating and registering it. Cached in
// an atomic pointer on the lock so the common path is a single atomic load.
func statFor(slot *atomic.Pointer[lockStat], name string) *lockStat {
	if st := slot.Load(); st != nil {
		return st
	}
	if name == "" {
		name = "?"
	}
	registryMu.Lock()
	st := registry[name]
	if st == nil {
		st = &lockStat{name: name}
		registry[name] = st
	}
	registryMu.Unlock()
	slot.Store(st)
	return st
}

func (s *lockStat) recordWait(d time.Duration) {
	s.acquisitions.Add(1)
	s.waitTotal.Add(int64(d))
	storeMax(&s.maxWait, int64(d))
	if d >= cfg.WaitWarn {
		s.contended.Add(1)
		logger.Warn("lock contention", "lock", s.name, "waited", d.String())
	}
}

func (s *lockStat) recordHold(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.holdTotal.Add(int64(d))
	storeMax(&s.maxHold, int64(d))
	if d >= cfg.HoldWarn {
		s.longHolds.Add(1)
		logger.Warn("long lock hold", "lock", s.name, "held", d.String())
	}
}

func storeMax(dst *atomic.Int64, v int64) {
	for {
		cur := dst.Load()
		if v <= cur || dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

// LockStat is the exported snapshot of one lock's counters.
type LockStat struct {
	Name         string `json:"name"`
	Acquisitions int64  `json:"acquisitions"`
	WaitAvgNanos int64  `json:"wait_avg_nanos"`
	WaitMaxNanos int64  `json:"wait_max_nanos"`
	HoldAvgNanos int64  `json:"hold_avg_nanos"`
	HoldMaxNanos int64  `json:"hold_max_nanos"`
	Contended    int64  `json:"contended"`
	LongHolds    int64  `json:"long_holds"`
}

// LockStats returns a snapshot of every named lock, sorted by name.
func LockStats() []LockStat {
	registryMu.Lock()
	names := make([]string, 0, len(registry))
	stats := make(map[string]*lockStat, len(registry))
	for n, s := range registry {
		names = append(names, n)
		stats[n] = s
	}
	registryMu.Unlock()
	sort.Strings(names)
	out := make([]LockStat, 0, len(names))
	for _, n := range names {
		s := stats[n]
		acq := s.acquisitions.Load()
		var waitAvg, holdAvg int64
		if acq > 0 {
			waitAvg = s.waitTotal.Load() / acq
			holdAvg = s.holdTotal.Load() / acq
		}
		out = append(out, LockStat{
			Name:         n,
			Acquisitions: acq,
			WaitAvgNanos: waitAvg,
			WaitMaxNanos: s.maxWait.Load(),
			HoldAvgNanos: holdAvg,
			HoldMaxNanos: s.maxHold.Load(),
			Contended:    s.contended.Load(),
			LongHolds:    s.longHolds.Load(),
		})
	}
	return out
}

// ---- lock-order graph (deadlock prediction) ------------------------------
//
// Two goroutines that take locks A and B in opposite orders can deadlock even if
// they never actually have yet. We catch that without waiting for the hang: each
// goroutine's currently-held locks are tracked, and every acquisition records a
// directed edge held -> acquiring. If adding an edge X -> Y closes a cycle (Y can
// already reach X), the orders are inconsistent and we warn once for that pair.

var (
	graphMu   sync.Mutex
	heldByG   = map[int64][]string{}         // goroutine -> stack of held lock names
	edges     = map[string]map[string]bool{} // from -> set(to)
	reported  = map[string]bool{}            // "a|b" pairs already warned about
)

// enterAcquire is called just before a goroutine blocks to acquire `name` while
// holding whatever it already holds. It records the ordering edges and detects a
// cycle, so a latent deadlock is reported at the moment the bad order first
// occurs — not when it finally hangs.
func enterAcquire(g int64, name string) {
	graphMu.Lock()
	defer graphMu.Unlock()
	for _, held := range heldByG[g] {
		if held == name {
			continue // reentrant-ish; nothing new to learn
		}
		addEdgeLocked(held, name)
		if reachableLocked(name, held) {
			warnCycleLocked(held, name)
		}
	}
}

func markHeld(g int64, name string) {
	graphMu.Lock()
	heldByG[g] = append(heldByG[g], name)
	graphMu.Unlock()
}

func markReleased(g int64, name string) {
	graphMu.Lock()
	defer graphMu.Unlock()
	s := heldByG[g]
	// Remove the most recent occurrence (locks are released LIFO in practice).
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == name {
			heldByG[g] = append(s[:i], s[i+1:]...)
			break
		}
	}
	if len(heldByG[g]) == 0 {
		delete(heldByG, g)
	}
}

func addEdgeLocked(from, to string) {
	if edges[from] == nil {
		edges[from] = map[string]bool{}
	}
	edges[from][to] = true
}

// reachableLocked reports whether `to` is reachable from `from` following edges
// (a DFS). Used to test whether a new edge would close a cycle.
func reachableLocked(from, to string) bool {
	if from == to {
		return true
	}
	seen := map[string]bool{from: true}
	stack := []string{from}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for next := range edges[n] {
			if next == to {
				return true
			}
			if !seen[next] {
				seen[next] = true
				stack = append(stack, next)
			}
		}
	}
	return false
}

func warnCycleLocked(a, b string) {
	key := a + "|" + b
	rev := b + "|" + a
	if reported[key] || reported[rev] {
		return
	}
	reported[key] = true
	cycleCount.Add(1)
	logger.Error("POTENTIAL DEADLOCK: inconsistent lock order",
		"locks", fmt.Sprintf("%s <-> %s", a, b),
		"detail", fmt.Sprintf("%q is acquired while holding %q, but the reverse order also occurs", b, a))
}

var cycleCount atomic.Int64

// LockOrderWarning names an inconsistent-order lock pair that was detected.
type LockOrderWarning struct {
	A string `json:"a"`
	B string `json:"b"`
}

// LockOrderWarnings returns the distinct inconsistent-order pairs seen so far.
func LockOrderWarnings() []LockOrderWarning {
	graphMu.Lock()
	defer graphMu.Unlock()
	out := make([]LockOrderWarning, 0, len(reported))
	for k := range reported {
		if i := indexByte(k, '|'); i >= 0 {
			out = append(out, LockOrderWarning{A: k[:i], B: k[i+1:]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].A != out[j].A {
			return out[i].A < out[j].A
		}
		return out[i].B < out[j].B
	})
	return out
}

// CycleCount is the number of distinct inconsistent-order pairs detected.
func CycleCount() int64 { return cycleCount.Load() }

// resetGraph clears the lock-order graph and registry; used by tests.
func resetGraph() {
	graphMu.Lock()
	heldByG = map[int64][]string{}
	edges = map[string]map[string]bool{}
	reported = map[string]bool{}
	graphMu.Unlock()
	cycleCount.Store(0)
	registryMu.Lock()
	registry = map[string]*lockStat{}
	registryMu.Unlock()
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
