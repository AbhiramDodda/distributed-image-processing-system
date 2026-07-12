package consensus

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus/consensuspb"
)

// CommitCommand is the replicated record of a single exactly-once commit
// decision: task TaskID's output at FinalKey is committed as of lease Generation.
// It is the command the coordinator proposes to close the exactly-once gap -- the
// terminal "this task is done" transition becomes an entry in the Raft log rather
// than a local WAL write, so every coordinator agrees on it and a failover leader
// never re-dispatches a task that a majority already recorded as committed. This
// is the idiomatic in-memory type; its wire form is the consensuspb.CommitCommand
// protobuf (see Encode).
type CommitCommand struct {
	TaskID string
	JobID string
	Generation int64
	FinalKey string
}

// Encode renders the command as protobuf for Propose/Apply. It rides the Raft log
// alongside the raftpb messages the transport already carries, so a schema'd wire
// form (rather than ad-hoc JSON) keeps the durable log format consistent and
// evolvable. The idiomatic Go struct stays the in-memory type; consensuspb is only
// the wire representation.
func (c CommitCommand) Encode() ([]byte, error) {
	return proto.Marshal(&consensuspb.CommitCommand{
		TaskId: c.TaskID,
		JobId: c.JobID,
		Generation: c.Generation,
		FinalKey: c.FinalKey,
	})
}

// DecodeCommitCommand is the inverse of Encode.
func DecodeCommitCommand(data []byte) (CommitCommand, error) {
	var pb consensuspb.CommitCommand
	if err := proto.Unmarshal(data, &pb); err != nil {
		return CommitCommand{}, fmt.Errorf("consensus: decode commit command: %w", err)
	}
	if pb.GetTaskId() == "" {
		return CommitCommand{}, fmt.Errorf("consensus: commit command missing task_id")
	}
	return CommitCommand{
		TaskID: pb.GetTaskId(),
		JobID: pb.GetJobId(),
		Generation: pb.GetGeneration(),
		FinalKey: pb.GetFinalKey(),
	}, nil
}

// CommitFSM is the coordinator's replicated state machine for exactly-once commit
// decisions. Apply is deterministic -- the same command applied on every node
// produces the same record -- which is what makes the replicas agree. It is the
// real-workload counterpart to the reference KVStore.
type CommitFSM struct {
	mu sync.RWMutex
	committed map[string]CommitCommand
}

// NewCommitFSM returns an empty commit ledger.
func NewCommitFSM() *CommitFSM {
	return &CommitFSM{committed: make(map[string]CommitCommand)}
}

// Apply records a commit decision. It is:
//   - idempotent: re-applying the same (or an equal-generation) command for a
//     task leaves exactly one record, so an at-least-once retry commits once.
//   - fenced: a command whose Generation is not strictly greater than the record
//     already held for that task is ignored, so a stale/zombie attempt (an older
//     lease generation, e.g. a report that lost a work-stealing race) can never
//     overwrite a newer commit.
//
// Because both rules depend only on the command and the current record -- never on
// wall-clock time or arrival order across nodes -- every replica converges to the
// same winner.
func (f *CommitFSM) Apply(data []byte) error {
	cmd, err := DecodeCommitCommand(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.committed[cmd.TaskID]; ok && cmd.Generation <= cur.Generation {
		return nil // fenced / idempotent: keep the existing (>=) record
	}
	f.committed[cmd.TaskID] = cmd
	return nil
}

// Committed returns the winning commit record for taskID, if any.
func (f *CommitFSM) Committed(taskID string) (CommitCommand, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, ok := f.committed[taskID]
	return c, ok
}

// Len returns the number of distinct committed tasks -- primarily for tests and
// diagnostics.
func (f *CommitFSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.committed)
}
