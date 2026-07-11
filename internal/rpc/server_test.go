package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/abhiramd/petabyte-platform/internal/rpc/coordinatorpb"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// fakeService is an in-memory JobService stub the RPC tests drive directly.
type fakeService struct {
	mu sync.Mutex
	jobs map[string]*scheduler.Job
	seq int
	assignment *scheduler.TaskAssignment
	started []string
	results []scheduler.ResultRequest
}

func newFakeService() *fakeService {
	return &fakeService{jobs: make(map[string]*scheduler.Job)}
}

func (f *fakeService) Submit(req scheduler.SubmitJobRequest) (*scheduler.Job, error) {
	if req.Dataset == "" {
		return nil, fmt.Errorf("dataset required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("job-%d", f.seq)
	job := &scheduler.Job{
		ID: id,
		Dataset: req.Dataset,
		Algorithm: req.Algorithm,
		Config: req.Config,
		Status: scheduler.JobPending,
		Shards: req.Shards,
		TotalTasks: len(req.Shards),
		CreatedAt: time.Unix(100, 0),
	}
	f.jobs[id] = job
	return job, nil
}

func (f *fakeService) GetJob(id string) (*scheduler.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %s not found", id)
	}
	cp := *j
	return &cp, nil
}

func (f *fakeService) ListJobs() []*scheduler.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*scheduler.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		cp := *j
		out = append(out, &cp)
	}
	return out
}

func (f *fakeService) PollTasks(string) (*scheduler.TaskAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.assignment, nil
}

func (f *fakeService) StartTask(taskID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, taskID)
	return nil
}

func (f *fakeService) ReportResult(_ context.Context, _ string, req scheduler.ResultRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, req)
	return nil
}

// setState mutates a job so WatchJob has transitions to stream.
func (f *fakeService) setState(id string, status scheduler.JobStatus, done int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	j.Status = status
	j.DoneTasks = done
}

func dialServer(t *testing.T, svc JobService, opts ...grpc.ServerOption) coordinatorpb.CoordinatorClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(opts...)
	coordinatorpb.RegisterCoordinatorServer(srv, NewServer(svc, 5*time.Millisecond))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return coordinatorpb.NewCoordinatorClient(conn)
}

func TestServer_submitAndGet(t *testing.T) {
	svc := newFakeService()
	cli := dialServer(t, svc)
	ctx := context.Background()

	resp, err := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{
		Dataset: "laion", Algorithm: "clip", Shards: []string{"a3", "7f"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if resp.GetTotalTasks() != 2 {
		t.Fatalf("total_tasks = %d, want 2", resp.GetTotalTasks())
	}

	job, err := cli.GetJob(ctx, &coordinatorpb.GetJobRequest{JobId: resp.GetJobId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.GetDataset() != "laion" || job.GetAlgorithm() != "clip" {
		t.Fatalf("job round-tripped wrong: %+v", job)
	}
	if job.GetCreatedAtUnixNs() != time.Unix(100, 0).UnixNano() {
		t.Fatalf("created_at not mapped: %d", job.GetCreatedAtUnixNs())
	}
}

func TestServer_submitInvalidArg(t *testing.T) {
	cli := dialServer(t, newFakeService())
	_, err := cli.SubmitJob(context.Background(), &coordinatorpb.SubmitJobRequest{Algorithm: "clip"})
	if err == nil {
		t.Fatal("expected error submitting job without dataset")
	}
}

func TestServer_getJobNotFound(t *testing.T) {
	cli := dialServer(t, newFakeService())
	if _, err := cli.GetJob(context.Background(), &coordinatorpb.GetJobRequest{JobId: "ghost"}); err == nil {
		t.Fatal("expected NotFound for unknown job")
	}
}

func TestServer_pollTasks(t *testing.T) {
	svc := newFakeService()
	cli := dialServer(t, svc)
	ctx := context.Background()

	resp, err := cli.PollTasks(ctx, &coordinatorpb.PollTasksRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("PollTasks empty: %v", err)
	}
	if resp.GetHasWork() {
		t.Fatal("expected no work when scheduler has none")
	}

	svc.assignment = &scheduler.TaskAssignment{TaskID: "t1", JobID: "j1", Shard: "a3", Dataset: "laion"}
	resp, err = cli.PollTasks(ctx, &coordinatorpb.PollTasksRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("PollTasks work: %v", err)
	}
	if !resp.GetHasWork() || resp.GetAssignment().GetTaskId() != "t1" {
		t.Fatalf("expected assignment t1, got %+v", resp.GetAssignment())
	}
}

func TestServer_pollTasksRequiresWorker(t *testing.T) {
	cli := dialServer(t, newFakeService())
	if _, err := cli.PollTasks(context.Background(), &coordinatorpb.PollTasksRequest{}); err == nil {
		t.Fatal("expected InvalidArgument without worker_id")
	}
}

// WatchJob must deliver an immediate first frame, then only changed frames, and
// close the stream once the job reaches a terminal state.
func TestServer_watchJobStreamsToTerminal(t *testing.T) {
	svc := newFakeService()
	cli := dialServer(t, svc)
	ctx := context.Background()

	sub, _ := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{Dataset: "laion", Shards: []string{"a3", "7f"}})
	id := sub.GetJobId()
	svc.setState(id, scheduler.JobRunning, 0)

	go func() {
		time.Sleep(15 * time.Millisecond)
		svc.setState(id, scheduler.JobRunning, 1)
		time.Sleep(15 * time.Millisecond)
		svc.setState(id, scheduler.JobCompleted, 2)
	}()

	stream, err := cli.WatchJob(ctx, &coordinatorpb.WatchJobRequest{JobId: id})
	if err != nil {
		t.Fatalf("WatchJob: %v", err)
	}
	var statuses []string
	for {
		job, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		statuses = append(statuses, job.GetStatus())
	}
	if len(statuses) == 0 {
		t.Fatal("no frames received")
	}
	if statuses[0] != string(scheduler.JobRunning) {
		t.Fatalf("first frame = %q, want running", statuses[0])
	}
	if last := statuses[len(statuses)-1]; last != string(scheduler.JobCompleted) {
		t.Fatalf("last frame = %q, want completed", last)
	}
}
