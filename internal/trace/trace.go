// Package trace is an opt-in causal event tracer for the scheduler/worker
// boundary. Ordinary logs record what one process did; this records the causal
// chain of a *unit of work* as it crosses processes and re-executes:
//
//   - a task is assigned, its lease is extended a chunk at a time, its untouched
//     tail is stolen into a child sub-task, it is retried after a failure, it is
//     committed;
//   - each of those is emitted as an Event carrying the work's identity, its
//     lease generation, the acting worker, and — for a steal or a retry — the
//     parent it descends from.
//
// The correlation handle is *derived from work identity*, not propagated over
// the wire. TraceID(jobID, shard, start, end) is a hash of the canonical
// (job, shard, range) triple, so the coordinator and the worker independently
// compute the SAME id from fields the assignment already carries — no proto
// change, no header plumbing. It is the same attempt-independent identity that
// makes FinalResultKey and SideEffectKey idempotent (see internal/scheduler).
// A per-attempt span is (TaskID, Generation, WorkerID); the causal Parent links
// a stolen child to its victim and a retry to its predecessor.
//
// Everything is a cheap no-op until Enable (or EnableFromEnv) flips the flag, so
// a production run pays only one atomic load per would-be emit. Turn it on to
// answer "why did this task run twice" across the coordinator and its workers,
// joined by TraceID.
package trace

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Event kinds. Kept as plain strings so the JSON is self-describing and a
// consumer needs no shared enum.
const (
	KindAssigned = "assigned"      // task handed to a worker on poll
	KindGranted = "chunk_granted" // lease extended to a new bound on renew
	KindStolen = "stolen"        // untouched tail split into a child sub-task
	KindRetry = "retry"         // failed task requeued for another attempt
	KindCommitted = "committed"     // result committed (terminal, success)
	KindFailed = "failed"        // task reached terminal failure
	KindWorkerRecv = "worker_recv"   // worker accepted an assignment
	KindStaged = "staged"        // worker staged its result object
)

// Event is one point on a unit of work's causal timeline. TraceID joins events
// for the same logical (job, shard, range) across processes; Parent joins a
// derived task (a steal child, a retry) back to the work it came from.
type Event struct {
	Time time.Time `json:"time"`
	Kind string `json:"kind"`
	TraceID string `json:"trace_id"`
	TaskID string `json:"task_id"`
	JobID string `json:"job_id,omitempty"`
	Shard string `json:"shard,omitempty"`
	Generation int64 `json:"generation"`
	WorkerID string `json:"worker_id,omitempty"`
	Parent string `json:"parent,omitempty"` // parent TaskID for steals/retries
	Attrs map[string]string `json:"attrs,omitempty"`
}

// on gates emission. Read on every would-be Emit, so it is an atomic load.
var on atomic.Bool

var clock = time.Now

const maxEvents = 4096

var (
	evCount atomic.Int64
	evMu sync.Mutex
	evRing [maxEvents]Event
	evNext int // next write index into the ring
)

// Enable turns tracing on. Safe to call once at startup; calling again is
// harmless. Off by default so a normal run is unaffected.
func Enable() { on.Store(true) }

// EnableFromEnv turns tracing on iff PETABYTE_TRACE is truthy. This is the
// intended switch: off by default, flipped on for a run without a rebuild. It
// logs through the given logger only to announce itself.
func EnableFromEnv(log *slog.Logger) bool {
	if truthy(os.Getenv("PETABYTE_TRACE")) {
		Enable()
		if log != nil {
			log.Info("event tracing enabled", "endpoint", "/debug/trace")
		}
		return true
	}
	return false
}

// Disable turns tracing off (used by tests to restore global state).
func Disable() { on.Store(false) }

// Enabled reports whether tracing is on. Guard the construction of an Event's
// Attrs map with this so an off run allocates nothing.
func Enabled() bool { return on.Load() }

// Emit records one event. It is a no-op when tracing is off. The Time field is
// stamped here if the caller left it zero. It never blocks on anything but the
// ring's own mutex, so it is safe to call while holding the scheduler lock (the
// order scheduler-lock -> trace-lock is consistent everywhere and never
// inverted, so it introduces no lock-order cycle).
func Emit(e Event) {
	if !on.Load() {
		return
	}
	if e.Time.IsZero() {
		e.Time = clock()
	}
	evCount.Add(1)
	evMu.Lock()
	evRing[evNext] = e
	evNext = (evNext + 1) % maxEvents
	evMu.Unlock()
}

// TraceID is the identity-derived correlation handle for a unit of work. It is a
// hash of the canonical (job, shard, [start,end)) triple, so any process holding
// those fields computes the identical id without coordination. A whole-shard
// task (end == -1 before the size is known) still gets a stable id for its
// range as reported.
func TraceID(jobID, shard string, start, end int64) string {
	h := sha256.New()
	h.Write([]byte(jobID))
	h.Write([]byte{0})
	h.Write([]byte(shard))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(start, 10)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(end, 10)))
	sum := h.Sum(nil)
	return "tr-" + hex.EncodeToString(sum[:12])
}

// Count is the total number of events emitted since start (may exceed the number
// retained in the ring).
func Count() int64 { return evCount.Load() }

// Events returns up to maxEvents most recent events, oldest first.
func Events() []Event {
	evMu.Lock()
	defer evMu.Unlock()
	total := evCount.Load()
	n := int(total)
	if n > maxEvents {
		n = maxEvents
	}
	out := make([]Event, 0, n)
	start := 0
	if total > maxEvents {
		start = evNext
	}
	for i := 0; i < n; i++ {
		out = append(out, evRing[(start+i)%maxEvents])
	}
	return out
}

// reset clears the ring; used by tests.
func reset() {
	evMu.Lock()
	defer evMu.Unlock()
	evCount.Store(0)
	evNext = 0
	evRing = [maxEvents]Event{}
}

func truthy(s string) bool {
	switch s {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	}
	return false
}
