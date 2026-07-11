// Package rpc is the gRPC face of the coordinator control plane (Level 6). The
// Server translates protobuf messages to and from the scheduler's own types and
// delegates to a JobService -- an interface the real *scheduler.Scheduler
// satisfies, which keeps this package testable with a fake and avoids importing
// coordinator internals.
package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abhiramd/petabyte-platform/internal/rpc/coordinatorpb"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// JobService is the slice of the scheduler the gRPC layer needs. Declaring it
// here (consumer-side) rather than in scheduler means the RPC package owns its
// own contract and can be exercised with a stub.
type JobService interface {
	Submit(req scheduler.SubmitJobRequest) (*scheduler.Job, error)
	GetJob(id string) (*scheduler.Job, error)
	ListJobs() []*scheduler.Job
	PollTasks(workerID string) (*scheduler.TaskAssignment, error)
	StartTask(taskID, workerID string) error
	ReportResult(ctx context.Context, taskID string, req scheduler.ResultRequest) error
}

// The production scheduler satisfies JobService as-is, so the gRPC server can be
// pointed straight at it with no adapter.
var _ JobService = (*scheduler.Scheduler)(nil)

// Server implements coordinatorpb.CoordinatorServer.
type Server struct {
	coordinatorpb.UnimplementedCoordinatorServer
	svc JobService
	// watchInterval is how often WatchJob re-reads job state. Kept small for
	// responsiveness; a push-based notification from the scheduler is the future
	// optimization (see WatchJob).
	watchInterval time.Duration
}

// NewServer builds a Server over svc. A non-positive watchInterval defaults to
// 500ms.
func NewServer(svc JobService, watchInterval time.Duration) *Server {
	if watchInterval <= 0 {
		watchInterval = 500 * time.Millisecond
	}
	return &Server{svc: svc, watchInterval: watchInterval}
}

func (s *Server) SubmitJob(_ context.Context, req *coordinatorpb.SubmitJobRequest) (*coordinatorpb.SubmitJobResponse, error) {
	job, err := s.svc.Submit(scheduler.SubmitJobRequest{
		Dataset: req.GetDataset(),
		Algorithm: req.GetAlgorithm(),
		Config: req.GetConfig(),
		Shards: req.GetShards(),
	})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &coordinatorpb.SubmitJobResponse{
		JobId: job.ID,
		TotalTasks: int32(job.TotalTasks),
	}, nil
}

func (s *Server) GetJob(_ context.Context, req *coordinatorpb.GetJobRequest) (*coordinatorpb.Job, error) {
	job, err := s.svc.GetJob(req.GetJobId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return jobToProto(job), nil
}

func (s *Server) ListJobs(_ context.Context, _ *coordinatorpb.ListJobsRequest) (*coordinatorpb.ListJobsResponse, error) {
	jobs := s.svc.ListJobs()
	out := make([]*coordinatorpb.Job, len(jobs))
	for i, j := range jobs {
		out[i] = jobToProto(j)
	}
	return &coordinatorpb.ListJobsResponse{Jobs: out}, nil
}

func (s *Server) PollTasks(_ context.Context, req *coordinatorpb.PollTasksRequest) (*coordinatorpb.PollTasksResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id required")
	}
	a, err := s.svc.PollTasks(req.GetWorkerId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &coordinatorpb.PollTasksResponse{
		Assignment: assignmentToProto(a),
		HasWork: a != nil,
	}, nil
}

func (s *Server) StartTask(_ context.Context, req *coordinatorpb.StartTaskRequest) (*coordinatorpb.StartTaskResponse, error) {
	if err := s.svc.StartTask(req.GetTaskId(), req.GetWorkerId()); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &coordinatorpb.StartTaskResponse{}, nil
}

func (s *Server) ReportResult(ctx context.Context, req *coordinatorpb.ReportResultRequest) (*coordinatorpb.ReportResultResponse, error) {
	err := s.svc.ReportResult(ctx, req.GetTaskId(), scheduler.ResultRequest{
		WorkerID: req.GetWorkerId(),
		ImagesProcessed: req.GetImagesProcessed(),
		BytesRead: req.GetBytesRead(),
		OutputKey: req.GetOutputKey(),
		Duration: time.Duration(req.GetDurationNs()),
		Error: req.GetError(),
	})
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &coordinatorpb.ReportResultResponse{}, nil
}

// WatchJob streams the job's state to the client until it reaches a terminal
// status (completed/failed/cancelled) or the client disconnects. It polls the
// scheduler on watchInterval and only sends when something changed, so a caller
// gets an immediate first frame and then deltas. This is a pragmatic push over a
// pull-based source; a channel the scheduler signals on each job mutation would
// remove the poll latency and is the natural follow-up.
func (s *Server) WatchJob(req *coordinatorpb.WatchJobRequest, stream coordinatorpb.Coordinator_WatchJobServer) error {
	ctx := stream.Context()
	job, err := s.svc.GetJob(req.GetJobId())
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	if err := stream.Send(jobToProto(job)); err != nil {
		return err
	}
	if isTerminal(job.Status) {
		return nil
	}

	last := jobSignature(job)
	ticker := time.NewTicker(s.watchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
			job, err := s.svc.GetJob(req.GetJobId())
			if err != nil {
				return status.Error(codes.NotFound, err.Error())
			}
			sig := jobSignature(job)
			if sig == last {
				continue
			}
			last = sig
			if err := stream.Send(jobToProto(job)); err != nil {
				return err
			}
			if isTerminal(job.Status) {
				return nil
			}
		}
	}
}

func isTerminal(s scheduler.JobStatus) bool {
	return s == scheduler.JobCompleted || s == scheduler.JobFailed || s == scheduler.JobCancelled
}

// jobSignature captures the fields WatchJob reports on, so an unchanged job is
// not re-sent. Progress counts plus status cover every visible transition.
func jobSignature(j *scheduler.Job) [4]int {
	return [4]int{int(statusOrd(j.Status)), j.DoneTasks, j.FailedTasks, j.TotalTasks}
}

func statusOrd(s scheduler.JobStatus) int {
	switch s {
	case scheduler.JobPending:
		return 0
	case scheduler.JobRunning:
		return 1
	case scheduler.JobCompleted:
		return 2
	case scheduler.JobFailed:
		return 3
	case scheduler.JobCancelled:
		return 4
	default:
		return -1
	}
}

func jobToProto(j *scheduler.Job) *coordinatorpb.Job {
	return &coordinatorpb.Job{
		Id: j.ID,
		Dataset: j.Dataset,
		Algorithm: j.Algorithm,
		Config: j.Config,
		Status: string(j.Status),
		Shards: j.Shards,
		TotalTasks: int32(j.TotalTasks),
		DoneTasks: int32(j.DoneTasks),
		FailedTasks: int32(j.FailedTasks),
		CreatedAtUnixNs: unixNano(&j.CreatedAt),
		StartedAtUnixNs: unixNano(j.StartedAt),
		FinishedAtUnixNs: unixNano(j.FinishedAt),
		Error: j.Error,
	}
}

func assignmentToProto(a *scheduler.TaskAssignment) *coordinatorpb.TaskAssignment {
	if a == nil {
		return nil
	}
	return &coordinatorpb.TaskAssignment{
		TaskId: a.TaskID,
		JobId: a.JobID,
		Shard: a.Shard,
		Dataset: a.Dataset,
		Algorithm: a.Algorithm,
		Config: a.Config,
	}
}

func unixNano(t *time.Time) int64 {
	if t == nil || t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
