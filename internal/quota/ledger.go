package quota

import (
	"sync"
	"time"
)

// Ledger accumulates monotonic per-tenant consumption for billing and reporting.
// Unlike the Enforcer's live gauge, these counters only ever grow: they answer
// "how much has this tenant consumed?" rather than "how much are they holding
// right now?". Keeping them in resource-seconds (CPU-seconds, GPU-seconds) and
// raw bytes lets the billing layer apply whatever price per unit it likes
// without the platform baking in a currency.
type Ledger struct {
	mu sync.Mutex
	stmt map[string]Statement
}

// Statement is a tenant's cumulative consumption since the ledger was created.
type Statement struct {
	CPUSeconds float64
	GPUSeconds float64
	BytesRead int64
	BytesWritten int64
	JobsCompleted int64
}

// NewLedger returns an empty Ledger.
func NewLedger() *Ledger {
	return &Ledger{stmt: make(map[string]Statement)}
}

// RecordCompute charges a completed job's resource-time to a tenant. dur is the
// job's wall-clock runtime; the request's CPU and GPU counts are multiplied by
// it to yield resource-seconds. A job holding 8 cores for 10s costs 80
// CPU-seconds regardless of how busy those cores actually were -- reservation,
// not utilisation, is what the tenant denied to others.
func (l *Ledger) RecordCompute(tenant string, req Request, dur time.Duration) {
	secs := dur.Seconds()
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stmt[tenant]
	s.CPUSeconds += req.CPUCores * secs
	s.GPUSeconds += float64(req.GPUCount) * secs
	s.JobsCompleted++
	l.stmt[tenant] = s
}

// RecordIO charges bytes moved to a tenant. Negative counts are ignored so a
// buggy caller cannot credit a tenant back down.
func (l *Ledger) RecordIO(tenant string, bytesRead, bytesWritten int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stmt[tenant]
	if bytesRead > 0 {
		s.BytesRead += bytesRead
	}
	if bytesWritten > 0 {
		s.BytesWritten += bytesWritten
	}
	l.stmt[tenant] = s
}

// Statement returns a tenant's cumulative consumption. An unknown tenant yields
// the zero Statement.
func (l *Ledger) Statement(tenant string) Statement {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stmt[tenant]
}
