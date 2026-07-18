// Package admission is the platform's backpressure layer. Where the Level 6
// quota.Enforcer answers "does this tenant have resource headroom?", the
// admission Controller answers the orthogonal load question: "can the platform
// take another job right now without overrunning its stable operating point?"
//
// The mechanism is deliberately load-shedding, not queuing. A bounded global
// in-flight cap is the backpressure valve: once the platform is saturated,
// further submissions are rejected (a fast, explicit 429) instead of being
// queued unboundedly. Unbounded queues are how a system that looks healthy at
// steady state collapses under a burst -- latency grows without limit while the
// queue drains (Little's Law: L = lambda x W, so a queue that never empties has
// unbounded W). Shedding keeps the scheduler's working set, and therefore its
// tail latency, bounded regardless of arrival rate.
//
// Capacity is partitioned across tenants by weight, so one tenant's burst cannot
// starve another: each tenant gets a guaranteed slice of the global cap and is
// shed once it exceeds that slice, even if the platform as a whole has room. That
// makes each tenant an independent Erlang-B loss system (see the README) whose
// blocking probability is a closed-form function of its offered load and share.
package admission

import (
	"errors"
	"fmt"
	"sync"
)

// ErrRejected wraps every admission denial so callers can map it to a single
// backpressure signal (HTTP 429) with errors.Is, while still surfacing a
// human-readable reason.
var ErrRejected = errors.New("admission rejected")

// Class is a tenant's admission class. Weight sets both its guaranteed slice of
// the global in-flight capacity and, at the scheduler, its dispatch priority: a
// higher weight buys more concurrency and earlier service under contention.
type Class struct {
	Weight int
}

// Config configures a Controller.
type Config struct {
	// MaxInFlight is the global ceiling on concurrently-admitted (in-flight) jobs.
	// It is the single knob that bounds the platform's working set; pick it from
	// the point past which the scheduler's latency stops being flat (the knee of
	// the load curve), not from raw resource totals.
	MaxInFlight int
}

// Controller admits or sheds job submissions against a bounded global capacity
// partitioned across tenants by weight. It fails closed: a tenant with no
// configured class is rejected, so a misconfiguration never hands out unbounded
// admission.
type Controller struct {
	mu sync.Mutex
	cfg Config
	classes map[string]Class
	totalWeight int
	inflight map[string]int
	total int
	admitted int64
	rejected int64
}

// New returns a Controller with no tenants configured. A non-positive MaxInFlight
// is treated as 1 so the controller always sheds rather than admitting an
// unbounded working set.
func New(cfg Config) *Controller {
	if cfg.MaxInFlight < 1 {
		cfg.MaxInFlight = 1
	}
	return &Controller{
		cfg: cfg,
		classes: make(map[string]Class),
		inflight: make(map[string]int),
	}
}

// SetClass registers or replaces a tenant's admission class. A weight below 1 is
// clamped to 1 so every configured tenant keeps at least a one-slot guarantee.
func (c *Controller) SetClass(tenant string, cl Class) {
	if cl.Weight < 1 {
		cl.Weight = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.classes[tenant]; ok {
		c.totalWeight -= old.Weight
	}
	c.classes[tenant] = cl
	c.totalWeight += cl.Weight
}

// shareLocked is the tenant's reserved slice of the global capacity, proportional
// to its weight and floored at 1 so a low-weight tenant is never starved to zero.
// Because the shares are weight-proportional they sum to at most MaxInFlight (up
// to rounding), so honouring every per-tenant share also honours the global cap;
// the global check in Admit is a hard backstop against rounding and races.
func (c *Controller) shareLocked(cl Class) int {
	if c.totalWeight == 0 {
		return c.cfg.MaxInFlight
	}
	s := c.cfg.MaxInFlight * cl.Weight / c.totalWeight
	if s < 1 {
		s = 1
	}
	return s
}

// Admit tries to reserve one in-flight slot for tenant. On success it returns a
// Ticket the caller must Release when the job reaches a terminal state. On
// failure it returns an error wrapping ErrRejected: either the tenant is at its
// weighted share or the global cap is full. Both check-and-reserve steps happen
// under one lock, so two concurrent submissions cannot both slip past a limit
// they jointly exceed.
func (c *Controller) Admit(tenant string) (*Ticket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cl, ok := c.classes[tenant]
	if !ok {
		c.rejected++
		return nil, fmt.Errorf("tenant %q has no admission class: %w", tenant, ErrRejected)
	}
	if share := c.shareLocked(cl); c.inflight[tenant]+1 > share {
		c.rejected++
		return nil, fmt.Errorf("tenant %q at concurrency share %d: %w", tenant, share, ErrRejected)
	}
	if c.total+1 > c.cfg.MaxInFlight {
		c.rejected++
		return nil, fmt.Errorf("global in-flight cap %d reached: %w", c.cfg.MaxInFlight, ErrRejected)
	}

	c.inflight[tenant]++
	c.total++
	c.admitted++
	return &Ticket{c: c, tenant: tenant}, nil
}

// Ticket is a held admission slot. Release frees it; it is idempotent so a defer
// plus an explicit release cannot double-count.
type Ticket struct {
	c *Controller
	tenant string
	once sync.Once
}

// Release returns the ticket's slot to its tenant and the global pool. Safe to
// call more than once.
func (t *Ticket) Release() {
	t.once.Do(func() {
		c := t.c
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.inflight[t.tenant] > 0 {
			c.inflight[t.tenant]--
			c.total--
		}
	})
}

// Stats is a snapshot of the controller's live load and lifetime counters.
type Stats struct {
	MaxInFlight int `json:"max_in_flight"`
	InFlight int `json:"in_flight"`
	Admitted int64 `json:"admitted_total"`
	Rejected int64 `json:"rejected_total"`
	PerTenant map[string]int `json:"per_tenant_in_flight"`
}

// Stats returns a point-in-time view of admission load, safe to serialise.
func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	per := make(map[string]int, len(c.inflight))
	for t, n := range c.inflight {
		if n > 0 {
			per[t] = n
		}
	}
	return Stats{
		MaxInFlight: c.cfg.MaxInFlight,
		InFlight: c.total,
		Admitted: c.admitted,
		Rejected: c.rejected,
		PerTenant: per,
	}
}
