package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// raftCommitDecider records a task's terminal commit through the Raft log, so the
// decision is agreed exactly once across coordinators and survives leader
// failure. It is the scheduler.CommitDecider that closes the gap the local WAL
// mark leaves open (see design.md §3.1). The adapter is the only place that
// bridges scheduler <-> consensus, keeping both packages independent of each other.
type raftCommitDecider struct {
	node *consensus.Node
	fsm *consensus.CommitFSM
	log *slog.Logger
}

// proposeInterval is how often Decide re-proposes while waiting for its command to
// be applied. Raft silently drops a proposal made during a leadership change, so a
// waiter must retry until it observes the effect in the FSM.
const proposeInterval = 20 * time.Millisecond

// Decide proposes the commit to the Raft cluster and returns once it (or a newer
// attempt) is applied to the local FSM. It is idempotent and fenced by the FSM:
// a duplicate report observes the existing record and returns immediately, and a
// stale generation loses to the recorded one.
func (d *raftCommitDecider) Decide(ctx context.Context, dec scheduler.CommitDecision) (scheduler.CommitDecision, error) {
	// Fast path: already agreed (a duplicate/late report, or a newer attempt won).
	if rec, ok := d.fsm.Committed(dec.TaskID); ok && rec.Generation >= dec.Generation {
		return fromRecord(rec), nil
	}
	if !d.node.IsLeader() {
		return scheduler.CommitDecision{}, fmt.Errorf("coordinator: not raft leader; cannot commit task %s", dec.TaskID)
	}

	cmd := consensus.CommitCommand{
		TaskID: dec.TaskID,
		JobID: dec.JobID,
		Generation: dec.Generation,
		FinalKey: dec.FinalKey,
	}
	data, err := cmd.Encode()
	if err != nil {
		return scheduler.CommitDecision{}, fmt.Errorf("coordinator: encode commit: %w", err)
	}
	if err := d.node.Propose(ctx, data); err != nil {
		return scheduler.CommitDecision{}, fmt.Errorf("coordinator: propose commit task %s: %w", dec.TaskID, err)
	}

	ticker := time.NewTicker(proposeInterval)
	defer ticker.Stop()
	for {
		if rec, ok := d.fsm.Committed(dec.TaskID); ok && rec.Generation >= dec.Generation {
			return fromRecord(rec), nil
		}
		select {
		case <-ctx.Done():
			return scheduler.CommitDecision{}, fmt.Errorf("coordinator: commit task %s not agreed before deadline: %w", dec.TaskID, ctx.Err())
		case <-ticker.C:
			// Re-propose only while leader; a dropped proposal is silent, so this is
			// how a proposal that raced a leadership change eventually lands.
			if d.node.IsLeader() {
				_ = d.node.Propose(ctx, data)
			}
		}
	}
}

func fromRecord(r consensus.CommitCommand) scheduler.CommitDecision {
	return scheduler.CommitDecision{
		TaskID: r.TaskID,
		JobID: r.JobID,
		Generation: r.Generation,
		FinalKey: r.FinalKey,
	}
}

// EnableRaftCommit routes terminal task commits through the Raft cluster: the
// commit decision becomes a replicated log entry rather than a local WAL write,
// so every coordinator agrees on it and a failover leader never re-dispatches a
// committed task. node must already be running with fsm as its FSM. Optional and
// independent of EnableResultCommit (the copy) -- with both, a result is copied
// idempotently and then committed by consensus. Call before Start.
func (c *Coordinator) EnableRaftCommit(node *consensus.Node, fsm *consensus.CommitFSM) {
	c.raftNode = node
	c.sched.AttachCommitDecider(&raftCommitDecider{node: node, fsm: fsm, log: c.log})
	c.log.Info("coordinator raft commit enabled (consensus-agreed exactly-once)")
}

// IsRaftLeader reports whether this coordinator's Raft node currently leads. It is
// false when raft is not enabled. Useful for gating which coordinator serves
// writes and for tests that drive submits at the leader.
func (c *Coordinator) IsRaftLeader() bool {
	return c.raftNode != nil && c.raftNode.IsLeader()
}
