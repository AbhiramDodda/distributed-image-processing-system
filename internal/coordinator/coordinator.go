package coordinator

import (
	"context"
	"log/slog"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// Coordinator wires together cluster membership and job scheduling.
// It is the single coordinator node for Level 2; Level 6 adds Raft HA.
type Coordinator struct {
	cfg *config.Config
	log *slog.Logger
	ring *cluster.Ring
	membership *cluster.Membership
	sched *scheduler.Scheduler
	stopCh chan struct{}
}

func New(cfg *config.Config, log *slog.Logger) *Coordinator {
	ring := cluster.NewRing(cfg.Coordinator.VnodesPerNode)
	mem := cluster.NewMembership(
		ring,
		cfg.Coordinator.SuspectTimeout,
		cfg.Coordinator.DeadTimeout,
		log,
	)
	sched := scheduler.New(ring, cfg.Coordinator.TaskMaxRetries, log)
	return &Coordinator{
		cfg:        cfg,
		log:        log,
		ring:       ring,
		membership: mem,
		sched:      sched,
		stopCh:     make(chan struct{}),
	}
}

func (c *Coordinator) Start(ctx context.Context) {
	go c.tickLoop(ctx)
	go c.failureEventLoop(ctx)
}

func (c *Coordinator) Stop() { close(c.stopCh) }

func (c *Coordinator) Membership() *cluster.Membership  { return c.membership }
func (c *Coordinator) Ring() *cluster.Ring              { return c.ring }
func (c *Coordinator) Scheduler() *scheduler.Scheduler  { return c.sched }

func (c *Coordinator) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.membership.Tick()
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Coordinator) failureEventLoop(ctx context.Context) {
	for {
		select {
		case ev := <-c.membership.Events():
			if ev.NewState == cluster.NodeDead {
				c.sched.RebalanceWorker(ev.NodeID)
			}
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}
