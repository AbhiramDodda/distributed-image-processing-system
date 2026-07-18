package diag

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Violation is a recorded invariant failure: an assertion that fired at runtime
// because state that "can't happen" happened. Kept in a bounded ring so the
// /debug/diag endpoint can show the most recent ones without unbounded growth.
type Violation struct {
	Time time.Time `json:"time"`
	Msg string `json:"msg"`
	Detail string `json:"detail"`
	Goid int64 `json:"goid"`
	Stack string `json:"stack"`
}

const maxViolations = 128

var (
	violCount atomic.Int64
	violMu sync.Mutex
	violRing [maxViolations]Violation
	violNext int // next write index into the ring
)

// Assert checks a runtime invariant. When diagnostics are on and cond is false,
// it records a Violation (with the offending detail and a stack trace) and logs
// it at error level; it never panics, so a diagnostic build stays up and keeps
// running past the bug so you can see everything else it triggers. When
// diagnostics are off it is a no-op — but the caller still evaluates cond and
// builds detail, so guard hot paths with Enabled().
//
// detailArgs are formatted lazily (only on failure) as key/value pairs, e.g.
// Assert(a<=b, "frontier past granted", "frontier", a, "granted", b).
func Assert(cond bool, msg string, detailArgs ...any) {
	if cond || !on.Load() {
		return
	}
	record(msg, kvString(detailArgs))
}

// Assertf is Assert with a printf-style detail string.
func Assertf(cond bool, msg, format string, args ...any) {
	if cond || !on.Load() {
		return
	}
	record(msg, fmt.Sprintf(format, args...))
}

func record(msg, detail string) {
	v := Violation{
		Time: clock(),
		Msg: msg,
		Detail: detail,
		Goid: goid(),
		Stack: shortStack(2),
	}
	violCount.Add(1)
	violMu.Lock()
	violRing[violNext] = v
	violNext = (violNext + 1) % maxViolations
	violMu.Unlock()
	logger.Error("INVARIANT VIOLATED", "invariant", msg, "detail", detail, "goid", v.Goid)
}

// ViolationCount is the total number of invariant violations since start (may
// exceed the number retained in the ring).
func ViolationCount() int64 { return violCount.Load() }

// RecentViolations returns up to maxViolations most recent violations, newest
// last.
func RecentViolations() []Violation {
	violMu.Lock()
	defer violMu.Unlock()
	total := violCount.Load()
	n := int(total)
	if n > maxViolations {
		n = maxViolations
	}
	out := make([]Violation, 0, n)
	// The ring's oldest retained entry is at violNext when full, else at 0.
	start := 0
	if total > maxViolations {
		start = violNext
	}
	for i := 0; i < n; i++ {
		out = append(out, violRing[(start+i)%maxViolations])
	}
	return out
}

// resetViolations clears the ring; used by tests.
func resetViolations() {
	violMu.Lock()
	defer violMu.Unlock()
	violCount.Store(0)
	violNext = 0
	violRing = [maxViolations]Violation{}
}

// kvString renders variadic key/value detail args as "k=v k=v". An odd trailing
// arg is emitted alone. Kept allocation-free-ish and only called on failure.
func kvString(args []any) string {
	if len(args) == 0 {
		return ""
	}
	var sb []byte
	for i := 0; i < len(args); i += 2 {
		if i > 0 {
			sb = append(sb, ' ')
		}
		sb = append(sb, fmt.Sprint(args[i])...)
		if i+1 < len(args) {
			sb = append(sb, '=')
			sb = append(sb, fmt.Sprint(args[i+1])...)
		}
	}
	return string(sb)
}
