package coordinator

import (
	"context"
	"log/slog"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/effect"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// CompletionEvent is emitted once per committed task -- the reference side effect
// the platform performs on a task's behalf after its commit is agreed. Key is the
// task's deterministic idempotency key; a downstream that dedupes on it observes
// the event exactly once even though the scheduler may deliver it more than once.
type CompletionEvent struct {
	Key string
	TaskID string
	JobID string
	FinalKey string
}

// EventEmitter delivers a completion event to a downstream (a broker, a webhook, a
// log). It may be invoked more than once for the same event across retries and
// failover; the effect barrier that fronts it guarantees the underlying emit runs
// at most once per idempotency key, so the emitter itself need not be idempotent.
type EventEmitter func(ctx context.Context, ev CompletionEvent) error

// completionSink is the scheduler.SideEffect that fires CompletionEvents through an
// idempotency ledger. Claim is the exactly-once barrier: only the first delivery
// for a given key runs emit; a duplicate (an at-least-once retry, a failover
// re-report) loses the claim and is a no-op success. It is the receiver-side half
// of the deterministic idempotency key the scheduler stamps on each delivery.
type completionSink struct {
	ledger effect.Ledger
	emit EventEmitter
	log *slog.Logger
}

// Apply implements scheduler.SideEffect. A losing claim means the event already
// fired for this unit of committed work, so it returns nil (the caller's
// at-least-once retry is satisfied). A failed emit does not consume the claim's
// retry semantics here -- MemLedger claims eagerly, modelling a sink whose emit is
// atomic with the claim; a non-atomic downstream would surface the error and be
// retried by the next delivery.
func (s *completionSink) Apply(ctx context.Context, key string, d scheduler.CommitDecision) error {
	if !s.ledger.Claim(key) {
		return nil
	}
	return s.emit(ctx, CompletionEvent{
		Key: key,
		TaskID: d.TaskID,
		JobID: d.JobID,
		FinalKey: d.FinalKey,
	})
}

// EnableSideEffects fires an exactly-once completion event for each committed task.
// emit is invoked at most once per task's idempotency key (deduped by ledger); a
// nil emit logs the event, and a nil ledger uses a fresh in-memory one. This turns
// the residual exactly-once gap into a safe at-least-once delivery behind a dedup
// barrier (see design.md §3.1). Optional and independent of the commit path; call
// before Start.
func (c *Coordinator) EnableSideEffects(emit EventEmitter, ledger effect.Ledger) {
	if emit == nil {
		emit = c.logCompletion
	}
	if ledger == nil {
		ledger = effect.NewMemLedger()
	}
	c.sched.AttachSideEffect(&completionSink{ledger: ledger, emit: emit, log: c.log})
	c.log.Info("coordinator side effects enabled (exactly-once completion events)")
}

// logCompletion is the default emitter: it records the event. Because it runs
// behind the ledger, exactly one line appears per committed task.
func (c *Coordinator) logCompletion(_ context.Context, ev CompletionEvent) error {
	c.log.Info("task committed",
		"event", "task.committed",
		"idempotency_key", ev.Key,
		"task_id", ev.TaskID,
		"job_id", ev.JobID,
		"final_key", ev.FinalKey,
	)
	return nil
}
