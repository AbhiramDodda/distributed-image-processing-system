// Package diag is an opt-in runtime concurrency-diagnostics layer. It exists to
// surface the two classes of concurrency bug that a distributed scheduler is
// prone to and that ordinary logging misses:
//
//   - deadlocks / lock contention — via instrumented Mutex/RWMutex that time how
//     long callers wait for and hold a lock, and that maintain a global
//     lock-acquisition-order graph so an inconsistent ordering (a latent
//     deadlock) is reported the first time it is observed, before it ever
//     actually hangs;
//   - logical races — via Assert, a runtime invariant check that, when a
//     documented invariant is violated (a range overlap, a stale lease
//     generation acting, a task leaving a terminal state), logs loudly with the
//     offending values and a stack trace instead of silently corrupting state.
//
// It deliberately does NOT try to detect data races: that is what `go test
// -race` and `go build -race` are for, and nothing at runtime can replace them.
// This package is complementary — it catches the logical and lock-ordering bugs
// that the race detector cannot see.
//
// Everything here is a cheap no-op until Enable (or EnableFromEnv) is called, so
// production runs pay only a single atomic load per lock operation and nothing
// for assertions. Turn it on for stress runs, chaos tests, and when chasing a
// hang.
package diag

import (
	"bytes"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

// on is the global enable flag. It is read on every instrumented lock operation,
// so it is an atomic load (not a mutex) to stay cheap on the hot path.
var on atomic.Bool

// logger is where violations and warnings are written once enabled. It is set
// before Enable flips the flag and is not mutated concurrently with reads.
var logger = slog.Default()

// Config tunes the thresholds above which the instrumented locks emit warnings.
// Zero values fall back to the defaults in Enable.
type Config struct {
	// WaitWarn is the lock-acquisition wait above which a contention warning is
	// logged (a caller blocked this long to get the lock).
	WaitWarn time.Duration
	// HoldWarn is the critical-section duration above which a long-hold warning
	// is logged (a caller kept the lock this long, stalling everyone behind it).
	HoldWarn time.Duration
}

var cfg = Config{WaitWarn: 250 * time.Millisecond, HoldWarn: 250 * time.Millisecond}

// clock is time.Now, indirected so tests can inject deterministic durations.
var clock = time.Now

// Enable turns diagnostics on with the given logger and config. It is safe to
// call once at startup; calling it again just refreshes the logger and config.
// Passing a nil logger keeps the current one (slog.Default() initially).
func Enable(log *slog.Logger, c Config) {
	if log != nil {
		logger = log
	}
	if c.WaitWarn > 0 {
		cfg.WaitWarn = c.WaitWarn
	}
	if c.HoldWarn > 0 {
		cfg.HoldWarn = c.HoldWarn
	}
	on.Store(true)
	logger.Info("diagnostics enabled", "wait_warn", cfg.WaitWarn, "hold_warn", cfg.HoldWarn)
}

// EnableFromEnv turns diagnostics on iff the PETABYTE_DIAG environment variable
// is set to a truthy value (1/true/yes/on). It is the intended production
// switch: off by default, flipped on for a diagnostic run without a rebuild.
func EnableFromEnv(log *slog.Logger) bool {
	if truthy(os.Getenv("PETABYTE_DIAG")) {
		Enable(log, Config{})
		return true
	}
	return false
}

// Disable turns diagnostics back off (used by tests to restore global state).
func Disable() { on.Store(false) }

// Enabled reports whether diagnostics are on. Hot call sites should guard
// invariant checks with this so the argument evaluation is skipped when off.
func Enabled() bool { return on.Load() }

func truthy(s string) bool {
	switch s {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	}
	return false
}

// goid returns the current goroutine's ID by parsing the runtime stack header.
// The runtime does not export this; it is a well-known debugging hack, used here
// only when diagnostics are on so its cost never touches production. The ID is
// used to attribute lock holds and to key each goroutine's held-lock set for
// order-cycle detection.
func goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Header looks like: "goroutine 123 [running]:"
	b := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		if id, err := strconv.ParseInt(string(b[:i]), 10, 64); err == nil {
			return id
		}
	}
	return 0
}

// shortStack returns a compact stack trace (skipping the diag frames) for
// attaching to a violation or warning.
func shortStack(skip int) string {
	var pcs [24]uintptr
	n := runtime.Callers(skip+2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var sb bytes.Buffer
	for {
		f, more := frames.Next()
		sb.WriteString(f.Function)
		sb.WriteString("\n\t")
		sb.WriteString(f.File)
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(f.Line))
		sb.WriteByte('\n')
		if !more {
			break
		}
	}
	return sb.String()
}
