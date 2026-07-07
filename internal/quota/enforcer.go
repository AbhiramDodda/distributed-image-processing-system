// Package quota provides per-tenant admission control and usage accounting for
// the multi-tenant platform API (Level 6). The Enforcer answers a single
// question at job-submit time -- "does this tenant have room for this job right
// now?" -- while the Ledger accumulates consumed resource-time for billing.
//
// The two are deliberately separate: admission is a live gauge (reserved
// capacity that rises and falls as jobs start and finish) whereas billing is a
// monotonic counter (resource-seconds that only ever grow). Mixing them would
// force one lock to serve two very different access patterns.
package quota

import (
	"fmt"
	"sync"
)

// Request is the resource footprint a single job asks to hold for its lifetime.
// It mirrors the fields a scheduler already knows about a job so the caller does
// not have to translate.
type Request struct {
	CPUCores float64
	MemoryGB int
	GPUCount int
}

// TenantQuota is the live ceiling a tenant may hold concurrently. It bounds
// in-flight resources, not lifetime consumption -- a tenant may run a million
// jobs over a year so long as no more than MaxActiveJobs (and their resources)
// are outstanding at once.
type TenantQuota struct {
	MaxCPUCores float64
	MaxMemoryGB int
	MaxGPUCount int
	MaxActiveJobs int
}

// Usage is a snapshot of what a tenant currently holds.
type Usage struct {
	CPUCores float64
	MemoryGB int
	GPUCount int
	ActiveJobs int
}

// Enforcer tracks live per-tenant usage and admits or rejects new reservations.
// It denies by default: a tenant with no configured quota cannot reserve
// anything, so a misconfiguration fails closed rather than handing out unbounded
// capacity.
type Enforcer struct {
	mu sync.Mutex
	quota map[string]TenantQuota
	usage map[string]Usage
}

// NewEnforcer returns an Enforcer with no tenants configured.
func NewEnforcer() *Enforcer {
	return &Enforcer{
		quota: make(map[string]TenantQuota),
		usage: make(map[string]Usage),
	}
}

// SetQuota registers or replaces a tenant's ceiling. Lowering a quota below a
// tenant's current usage is allowed and simply blocks new reservations until
// enough running jobs finish -- existing jobs are never forcibly evicted here.
func (e *Enforcer) SetQuota(tenant string, q TenantQuota) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.quota[tenant] = q
}

// Reservation is a held claim against a tenant's quota. Release returns the
// resources to the pool; it is idempotent so a defer plus an explicit release on
// the success path cannot double-count.
type Reservation struct {
	enforcer *Enforcer
	tenant string
	req Request
	once sync.Once
}

// Reserve atomically checks the request against the tenant's remaining headroom
// and, if it fits on every dimension, records it. The check and the record
// happen under one lock so two concurrent jobs that each fit individually cannot
// both slip past a ceiling they jointly exceed.
func (e *Enforcer) Reserve(tenant string, req Request) (*Reservation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, ok := e.quota[tenant]
	if !ok {
		return nil, fmt.Errorf("quota: tenant %q has no configured quota", tenant)
	}
	u := e.usage[tenant]

	if u.ActiveJobs+1 > q.MaxActiveJobs {
		return nil, fmt.Errorf("quota: tenant %q at active-job limit %d", tenant, q.MaxActiveJobs)
	}
	if u.CPUCores+req.CPUCores > q.MaxCPUCores {
		return nil, fmt.Errorf("quota: tenant %q cpu %.1f+%.1f exceeds %.1f", tenant, u.CPUCores, req.CPUCores, q.MaxCPUCores)
	}
	if u.MemoryGB+req.MemoryGB > q.MaxMemoryGB {
		return nil, fmt.Errorf("quota: tenant %q memory %d+%d exceeds %d", tenant, u.MemoryGB, req.MemoryGB, q.MaxMemoryGB)
	}
	if u.GPUCount+req.GPUCount > q.MaxGPUCount {
		return nil, fmt.Errorf("quota: tenant %q gpu %d+%d exceeds %d", tenant, u.GPUCount, req.GPUCount, q.MaxGPUCount)
	}

	u.ActiveJobs++
	u.CPUCores += req.CPUCores
	u.MemoryGB += req.MemoryGB
	u.GPUCount += req.GPUCount
	e.usage[tenant] = u

	return &Reservation{enforcer: e, tenant: tenant, req: req}, nil
}

// Release returns the reservation's resources to the tenant's pool. Calling it
// more than once is a no-op.
func (r *Reservation) Release() {
	r.once.Do(func() {
		e := r.enforcer
		e.mu.Lock()
		defer e.mu.Unlock()
		u := e.usage[r.tenant]
		u.ActiveJobs = max(u.ActiveJobs-1, 0)
		u.CPUCores = max(u.CPUCores-r.req.CPUCores, 0)
		u.MemoryGB = max(u.MemoryGB-r.req.MemoryGB, 0)
		u.GPUCount = max(u.GPUCount-r.req.GPUCount, 0)
		e.usage[r.tenant] = u
	})
}

// Usage returns a snapshot of the tenant's current live consumption.
func (e *Enforcer) Usage(tenant string) Usage {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.usage[tenant]
}
