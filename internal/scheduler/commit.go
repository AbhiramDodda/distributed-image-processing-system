package scheduler

import (
	"context"
	"fmt"
)

// Committer promotes a worker's staged output to its final, canonical location.
// It is the commit point of the two-phase result protocol: a worker writes its
// output to a staging key and reports the task done; the scheduler then calls
// Commit to make that output visible at the canonical key before recording the
// task as TaskDone in the WAL.
//
// Commit MUST be idempotent. A task can be reported more than once — an
// at-least-once network retry, or a rebalance that re-runs a task whose original
// result arrives late — and every one of those reports must leave exactly one
// final object. *storage.Client satisfies this via a server-side copy to the
// attempt-independent final key (see FinalResultKey). It is kept as a narrow
// interface so the scheduler does not depend on the storage package.
type Committer interface {
	Commit(ctx context.Context, stagingKey, finalKey string) error
}

// AttachCommitter enables the exactly-once commit path. Without a committer the
// scheduler still records results, but a worker's staged output is never
// promoted (at-least-once behavior). Call before the scheduler serves traffic.
func (s *Scheduler) AttachCommitter(c Committer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committer = c
}

// CommitDecision is a request to commit one task's result. Generation is the
// task's lease generation, which fences stale attempts: a decision that loses to
// a higher generation is not the authoritative one.
type CommitDecision struct {
	TaskID string
	JobID string
	Generation int64
	FinalKey string
}

// CommitDecider records the terminal commit decision for a task through an
// external agreement protocol -- in production, an entry in the replicated Raft
// log -- so the decision is made exactly once and every coordinator agrees on it.
// It closes the split-brain / non-deterministic-recovery gap the local WAL mark
// leaves open: once Decide returns a winner, that task is committed as a
// majority-agreed fact and a failover leader will not re-dispatch it.
//
// Decide MUST be idempotent and fenced: calling it twice for the same task
// returns the same winning decision, and a decision with an older Generation than
// one already recorded loses to it. The returned winner may therefore differ from
// the argument when a newer attempt already committed. Kept as a narrow interface
// so the scheduler stays independent of the consensus package.
type CommitDecider interface {
	Decide(ctx context.Context, d CommitDecision) (winner CommitDecision, err error)
}

// AttachCommitDecider routes terminal commits through a consensus-backed decider
// (see CommitDecider). Without one, a task is marked done by the local WAL path
// (single-node behavior, unchanged). Call before the scheduler serves traffic.
func (s *Scheduler) AttachCommitDecider(d CommitDecider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitDecider = d
}

// StagingResultKey is where a worker writes a task's output before it is
// committed. It is keyed by task, not by attempt: retries reuse the same task
// and produce deterministic output for the same shard, and PutObject is atomic
// per object, so a concurrent late attempt can only overwrite it with identical
// bytes — never a partial read. Generation-scoped staging becomes necessary only
// once work-stealing splits a shard across workers (differentiation item #2).
func StagingResultKey(jobID, taskID string) string {
	return fmt.Sprintf("staging/%s/%s.json", jobID, taskID)
}

// Range identifies the slice of a shard a (sub-)task covers, for naming its
// result. Split distinguishes a whole-shard task from one produced by
// work-stealing; Start/End are offsets into the shard's sorted key list.
type Range struct {
	Start int64
	End int64
	Split bool
}

// FinalResultKey is a (sub-)task's canonical output location. A whole-shard task
// (never split) keeps the flat key results/{job}/{shard}.json, so jobs that
// never split are byte-for-byte unchanged and re-committing overwrites one
// object per shard. A split task encodes its range so a shard's pieces occupy
// distinct, still-deterministic keys — re-committing a given sub-range remains
// idempotent, and the ranges tile the shard exactly once.
func FinalResultKey(jobID, shard string, rng Range) string {
	if !rng.Split {
		return fmt.Sprintf("results/%s/%s.json", jobID, shard)
	}
	return fmt.Sprintf("results/%s/%s/%012d-%012d.json", jobID, shard, rng.Start, rng.End)
}
