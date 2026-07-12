package diag

import (
	"encoding/json"
	"net/http"
	"runtime"
)

// Snapshot is the full diagnostics report served at /debug/diag.
type Snapshot struct {
	Enabled         bool               `json:"enabled"`
	NumGoroutines   int                `json:"num_goroutines"`
	ViolationCount  int64              `json:"violation_count"`
	Violations      []Violation        `json:"violations"`
	Locks           []LockStat         `json:"locks"`
	LockOrderIssues []LockOrderWarning `json:"lock_order_issues"`
	GoroutineDump   string             `json:"goroutine_dump,omitempty"`
}

// Report builds the current diagnostics snapshot. Pass withStacks to include a
// full goroutine dump (useful when chasing a hang; larger payload).
func Report(withStacks bool) Snapshot {
	s := Snapshot{
		Enabled:         Enabled(),
		NumGoroutines:   runtime.NumGoroutine(),
		ViolationCount:  ViolationCount(),
		Violations:      RecentViolations(),
		Locks:           LockStats(),
		LockOrderIssues: LockOrderWarnings(),
	}
	if withStacks {
		s.GoroutineDump = goroutineDump()
	}
	return s
}

// Handler serves the diagnostics snapshot as JSON. GET /debug/diag?stacks=1
// includes a full goroutine dump. It works (returning enabled=false and a
// goroutine count) even when diagnostics are off, so it is always safe to
// register.
func Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		withStacks := truthy(r.URL.Query().Get("stacks"))
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(Report(withStacks))
	}
}

// goroutineDump returns every goroutine's stack — the go-to artifact for a hang,
// showing exactly which goroutines are blocked and on what.
func goroutineDump() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}
