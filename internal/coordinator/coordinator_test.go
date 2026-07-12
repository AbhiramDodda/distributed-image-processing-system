package coordinator_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/coordinator"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

var testLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := &config.Config{
		Coordinator: config.CoordinatorConfig{
			SuspectTimeout: 10 * time.Second,
			DeadTimeout:    20 * time.Second,
			VnodesPerNode:  50,
			TaskMaxRetries: 2,
		},
	}
	coord := coordinator.New(cfg, testLog)
	mux := http.NewServeMux()
	coordinator.NewAPI(coord).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func TestCoordinator_healthz(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestCoordinator_registerAndNodes(t *testing.T) {
	srv := newTestServer(t)

	resp := postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{
		ID: "w1", Address: "localhost:9001",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", resp.StatusCode)
	}

	nodesResp, err := http.Get(srv.URL + "/v1/cluster/nodes")
	if err != nil {
		t.Fatalf("GET nodes: %v", err)
	}
	var nodes []cluster.NodeInfo
	decode(t, nodesResp, &nodes)
	if len(nodes) != 1 {
		t.Fatalf("node count = %d, want 1", len(nodes))
	}
	if nodes[0].ID != "w1" {
		t.Errorf("node ID = %q, want w1", nodes[0].ID)
	}
}

func TestCoordinator_submitAndGetJob(t *testing.T) {
	srv := newTestServer(t)

	resp := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset:   "train",
		Algorithm: "resnet",
		Shards:    []string{"00", "01", "02"},
	})
	var submitResp scheduler.SubmitJobResponse
	decode(t, resp, &submitResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit status = %d", resp.StatusCode)
	}
	if submitResp.TotalTasks != 3 {
		t.Errorf("TotalTasks = %d, want 3", submitResp.TotalTasks)
	}

	jobResp, _ := http.Get(fmt.Sprintf("%s/v1/jobs/%s", srv.URL, submitResp.JobID))
	var job scheduler.Job
	decode(t, jobResp, &job)
	if job.ID != submitResp.JobID {
		t.Errorf("job ID mismatch: got %q, want %q", job.ID, submitResp.JobID)
	}
}

func TestCoordinator_fullJobLifecycle(t *testing.T) {
	srv := newTestServer(t)

	// Register a worker
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{
		ID: "w1", Address: "localhost:9001",
	})

	// Submit a job with 2 shards
	resp := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa", "bb"},
	})
	var submitResp scheduler.SubmitJobResponse
	decode(t, resp, &submitResp)
	jobID := submitResp.JobID

	// Worker polls and completes both tasks
	for i := 0; i < 2; i++ {
		pollResp, _ := http.Get(fmt.Sprintf("%s/v1/tasks/poll?worker=w1", srv.URL))
		var pr scheduler.PollResponse
		decode(t, pollResp, &pr)
		if !pr.HasWork || pr.Assignment == nil {
			t.Fatalf("poll %d: no work available", i)
		}
		taskID := pr.Assignment.TaskID

		startResp := postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/start", srv.URL, taskID),
			scheduler.StartTaskRequest{WorkerID: "w1"})
		startResp.Body.Close()

		resultResp := postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/result", srv.URL, taskID),
			scheduler.ResultRequest{WorkerID: "w1", ImagesProcessed: 50, BytesRead: 1024 * 1024})
		resultResp.Body.Close()
	}

	// Third poll should find no work
	pollResp, _ := http.Get(fmt.Sprintf("%s/v1/tasks/poll?worker=w1", srv.URL))
	var pr scheduler.PollResponse
	decode(t, pollResp, &pr)
	if pr.HasWork {
		t.Error("expected no work after all tasks completed")
	}

	// Job should be completed
	jobResp, _ := http.Get(fmt.Sprintf("%s/v1/jobs/%s", srv.URL, jobID))
	var job scheduler.Job
	decode(t, jobResp, &job)
	if job.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", job.Status)
	}
	if job.DoneTasks != 2 {
		t.Errorf("DoneTasks = %d, want 2", job.DoneTasks)
	}
}

// TestCoordinator_workStealingOverHTTP is the pass-B end-to-end check: a steal
// only fires in production once a worker drives the lease-renewal loop over HTTP.
// One worker takes a whole shard and renews (reporting the shard size, which fixes
// the range and grants its next chunk); a second, otherwise-idle worker then polls
// and receives a split sub-task carved from the busy worker's un-granted tail.
func TestCoordinator_workStealingOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{ID: "w0", Address: "localhost:9000"}).Body.Close()
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"}).Body.Close()

	resp := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa"},
	})
	var submit scheduler.SubmitJobResponse
	decode(t, resp, &submit)

	// w0 takes the whole shard. RangeEnd is -1 until it reports the shard size.
	pollResp, _ := http.Get(srv.URL + "/v1/tasks/poll?worker=w0")
	var pr scheduler.PollResponse
	decode(t, pollResp, &pr)
	if !pr.HasWork || pr.Assignment == nil {
		t.Fatal("w0 got no work")
	}
	a0 := pr.Assignment
	if a0.RangeEnd != -1 {
		t.Fatalf("fresh whole-shard task RangeEnd = %d, want -1", a0.RangeEnd)
	}

	// w0 renews at the end of its first granted chunk, reporting a 5000-item shard.
	// That fixes RangeEnd=5000 and grants the next chunk, leaving a stealable tail.
	renewResp := postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/renew", srv.URL, a0.TaskID),
		scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a0.Generation, Frontier: a0.Bound, Total: 5000})
	var lr scheduler.LeaseRenewal
	decode(t, renewResp, &lr)
	if lr.Bound <= a0.Bound {
		t.Fatalf("renewed bound = %d, want > %d", lr.Bound, a0.Bound)
	}
	if lr.Stolen {
		t.Fatal("w0 reported stolen before any steal happened")
	}

	// w1 is idle with no pending queue, so its poll must steal w0's un-granted tail.
	poll1, _ := http.Get(srv.URL + "/v1/tasks/poll?worker=w1")
	var pr1 scheduler.PollResponse
	decode(t, poll1, &pr1)
	if !pr1.HasWork || pr1.Assignment == nil {
		t.Fatal("w1 got no work: expected a stolen tail")
	}
	a1 := pr1.Assignment
	if a1.TaskID == a0.TaskID {
		t.Fatal("w1 received w0's task, not a split sub-task")
	}
	if !a1.Split {
		t.Error("stolen assignment not marked Split")
	}
	if a1.RangeStart <= lr.Bound || a1.RangeEnd != 5000 {
		t.Errorf("stolen range = [%d,%d), want start > %d and end 5000", a1.RangeStart, a1.RangeEnd, lr.Bound)
	}

	// w0's next renewal learns the steal: its Generation is now behind, so Stolen.
	renew2 := postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/renew", srv.URL, a0.TaskID),
		scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a0.Generation, Frontier: lr.Bound})
	var lr2 scheduler.LeaseRenewal
	decode(t, renew2, &lr2)
	if !lr2.Stolen {
		t.Error("w0 renewal after steal: Stolen = false, want true")
	}
	// The victim is now bounded by the split: it may still be granted forward, but
	// never past where its tail was handed off (that region belongs to w1 now).
	if lr2.Bound > a1.RangeStart {
		t.Errorf("victim bound after steal = %d, must not exceed split point %d", lr2.Bound, a1.RangeStart)
	}
}

func TestCoordinator_heartbeatUpdatesMetrics(t *testing.T) {
	srv := newTestServer(t)
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{
		ID: "w1", Address: "localhost:9001",
	})

	hb := cluster.Heartbeat{
		NodeID:  "w1",
		Metrics: cluster.NodeMetrics{ActiveTasks: 7, CPUPct: 65.5},
	}
	resp := postJSON(t, srv.URL+"/v1/cluster/heartbeat", hb)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("heartbeat status = %d, want 200", resp.StatusCode)
	}

	nodesResp, _ := http.Get(srv.URL + "/v1/cluster/nodes")
	var nodes []cluster.NodeInfo
	decode(t, nodesResp, &nodes)
	if nodes[0].Metrics.ActiveTasks != 7 {
		t.Errorf("ActiveTasks = %d, want 7", nodes[0].Metrics.ActiveTasks)
	}
}

func TestCoordinator_pendingMetric(t *testing.T) {
	srv := newTestServer(t)
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{
		ID: "w1", Address: "localhost:9001",
	})
	postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "ds", Algorithm: "alg", Shards: []string{"00", "01", "02"},
	})

	resp, err := http.Get(srv.URL + "/v1/metrics/pending")
	if err != nil {
		t.Fatalf("GET /v1/metrics/pending: %v", err)
	}
	var m map[string]any
	decode(t, resp, &m)

	if m["pending_tasks"].(float64) != 3 {
		t.Errorf("pending_tasks = %v, want 3", m["pending_tasks"])
	}
	if m["active_workers"].(float64) != 1 {
		t.Errorf("active_workers = %v, want 1", m["active_workers"])
	}
	if m["pending_tasks_per_worker"].(float64) != 3 {
		t.Errorf("pending_tasks_per_worker = %v, want 3", m["pending_tasks_per_worker"])
	}
}

func TestCoordinator_operatorDrain(t *testing.T) {
	srv := newTestServer(t)
	postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "ds", Algorithm: "alg", Shards: []string{"00", "01", "02", "03", "04"},
	})

	resp := postJSON(t, srv.URL+"/v1/operator/drain?n=3", nil)
	var assignments []scheduler.TaskAssignment
	decode(t, resp, &assignments)
	if len(assignments) != 3 {
		t.Fatalf("drain returned %d assignments, want 3", len(assignments))
	}

	// Pending count should drop to 2
	metricResp, _ := http.Get(srv.URL + "/v1/metrics/pending")
	var m map[string]any
	decode(t, metricResp, &m)
	if m["pending_tasks"].(float64) != 2 {
		t.Errorf("pending_tasks after drain = %v, want 2", m["pending_tasks"])
	}
}

func TestCoordinator_ringEndpoint(t *testing.T) {
	srv := newTestServer(t)
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{
		ID: "w1", Address: "localhost:9001",
	})

	resp, _ := http.Get(srv.URL + "/v1/cluster/ring")
	var ring map[string]any
	decode(t, resp, &ring)
	if ring["node_count"].(float64) != 1 {
		t.Errorf("ring node_count = %v, want 1", ring["node_count"])
	}
}
