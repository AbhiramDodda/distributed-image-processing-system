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

// StagingResultKey is where a worker writes a task's output before it is
// committed. It is keyed by task, not by attempt: retries reuse the same task
// and produce deterministic output for the same shard, and PutObject is atomic
// per object, so a concurrent late attempt can only overwrite it with identical
// bytes — never a partial read. Generation-scoped staging becomes necessary only
// once work-stealing splits a shard across workers (differentiation item #2).
func StagingResultKey(jobID, taskID string) string {
	return fmt.Sprintf("staging/%s/%s.json", jobID, taskID)
}

// FinalResultKey is a task's canonical output location. It embeds neither the
// task ID nor the attempt, so re-committing — after a retry, a rebalance, or a
// duplicate report — overwrites the same object: the results/ prefix holds
// exactly one object per (job, shard).
func FinalResultKey(jobID, shard string) string {
	return fmt.Sprintf("results/%s/%s.json", jobID, shard)
}
