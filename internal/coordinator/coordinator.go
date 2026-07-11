package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/pipeline"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
	"github.com/abhiramd/petabyte-platform/internal/storage"
)

// Coordinator wires together cluster membership and job scheduling.
// It is the single coordinator node for Level 2; Level 6 adds Raft HA.
type Coordinator struct {
	cfg *config.Config
	log *slog.Logger
	ring *cluster.Ring
	membership *cluster.Membership
	sched *scheduler.Scheduler
	wal *pipeline.WAL
	checkpointInterval time.Duration
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

// EnablePersistence opens a WAL under dir, restores the scheduler from it, and
// attaches it so future state changes are durable. Checkpoints are written
// every interval (a non-positive interval falls back to 30s). It must be called
// before Start, and only once.
func (c *Coordinator) EnablePersistence(dir string, interval time.Duration) error {
	w, rec, err := pipeline.OpenWAL(dir, c.log)
	if err != nil {
		return fmt.Errorf("coordinator: open wal: %w", err)
	}
	if err := c.sched.Restore(rec.Snapshot, rec.Records); err != nil {
		w.Close()
		return fmt.Errorf("coordinator: restore scheduler: %w", err)
	}
	c.sched.AttachStore(w)
	c.wal = w
	if interval <= 0 {
		interval = 30 * time.Second
	}
	c.checkpointInterval = interval
	c.log.Info("coordinator persistence enabled", "dir", dir, "checkpoint_interval", interval)
	return nil
}

// EnableResultCommit wires the exactly-once commit path: the scheduler promotes
// each worker's staged output to its canonical key (a server-side copy) before
// marking the task done. Optional — without it the coordinator records results
// at-least-once and staged output is never promoted. Call before Start.
func (c *Coordinator) EnableResultCommit(store *storage.Client) {
	c.sched.AttachCommitter(storeCommitter{store: store, log: c.log})
	c.log.Info("coordinator result-commit enabled (exactly-once staging)")
}

// storeCommitter promotes a staged result to its final key with a server-side
// copy, then best-effort deletes the staging object (the "move" of the two-phase
// commit). The copy is the durable step; a failed staging delete only leaves an
// orphan for a lifecycle rule or sweeper to reclaim, so it is logged, not fatal.
type storeCommitter struct {
	store *storage.Client
	log *slog.Logger
}

func (c storeCommitter) Commit(ctx context.Context, stagingKey, finalKey string) error {
	if err := c.store.Copy(ctx, stagingKey, finalKey); err != nil {
		return err
	}
	if err := c.store.Delete(ctx, stagingKey); err != nil {
		c.log.Warn("staging cleanup failed", "key", stagingKey, "err", err)
	}
	return nil
}

func (c *Coordinator) Start(ctx context.Context) {
	go c.tickLoop(ctx)
	go c.failureEventLoop(ctx)
	if c.wal != nil {
		go c.checkpointLoop(ctx)
	}
}

func (c *Coordinator) Stop() {
	close(c.stopCh)
	if c.wal != nil {
		if err := c.checkpoint(); err != nil {
			c.log.Error("coordinator: final checkpoint", "err", err)
		}
		c.wal.Close()
	}
}

func (c *Coordinator) checkpointLoop(ctx context.Context) {
	ticker := time.NewTicker(c.checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.checkpoint(); err != nil {
				c.log.Error("coordinator: checkpoint", "err", err)
			}
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Coordinator) checkpoint() error {
	snap, err := c.sched.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot scheduler: %w", err)
	}
	return c.wal.Checkpoint(snap)
}

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
