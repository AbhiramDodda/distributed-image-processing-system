package coordinator_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/admission"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/coordinator"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// newAdmissionServer builds a coordinator with a single-slot admission controller
// so a second submission is shed while the first is in flight.
func newAdmissionServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := &config.Config{
		Coordinator: config.CoordinatorConfig{
			SuspectTimeout: 10 * time.Second,
			DeadTimeout: 20 * time.Second,
			VnodesPerNode: 50,
			TaskMaxRetries: 2,
		},
	}
	coord := coordinator.New(cfg, testLog)
	ctrl := admission.New(admission.Config{MaxInFlight: 1})
	ctrl.SetClass("default", admission.Class{Weight: 1})
	coord.EnableAdmission(ctrl)

	mux := http.NewServeMux()
	coordinator.NewAPI(coord).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAdmission_shedsThenReadmitsAfterCompletion(t *testing.T) {
	srv := newAdmissionServer(t)
	postJSON(t, srv.URL+"/v1/cluster/register", cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"}).Body.Close()

	// First job fills the single admission slot.
	resp := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first submit status = %d, want 201", resp.StatusCode)
	}
	var first scheduler.SubmitJobResponse
	decode(t, resp, &first)

	// Second submission is shed while the first is in flight.
	shed := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"bb"},
	})
	if shed.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second submit status = %d, want 429 (backpressure)", shed.StatusCode)
	}
	if shed.Header.Get("Retry-After") == "" {
		t.Error("429 missing Retry-After header")
	}
	shed.Body.Close()

	// Admission metrics reflect the load.
	if in, adm, rej := admissionStats(t, srv); in != 1 || adm != 1 || rej != 1 {
		t.Fatalf("stats in=%d admitted=%d rejected=%d, want 1/1/1", in, adm, rej)
	}

	// Drive the first job to completion; its terminal transition releases the slot.
	completeJob(t, srv, "w1")

	// The slot is free, so a new submission is admitted again.
	readmit := postJSON(t, srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"cc"},
	})
	if readmit.StatusCode != http.StatusCreated {
		t.Fatalf("post-completion submit status = %d, want 201 (slot freed)", readmit.StatusCode)
	}
	readmit.Body.Close()
	if in, _, _ := admissionStats(t, srv); in != 1 {
		t.Fatalf("in-flight after readmit = %d, want 1", in)
	}
}

// completeJob polls, starts, and reports success for every pending task the
// worker can get, until none remain -- draining the one submitted job.
func completeJob(t *testing.T, srv *httptest.Server, worker string) {
	t.Helper()
	for {
		pollResp, _ := http.Get(fmt.Sprintf("%s/v1/tasks/poll?worker=%s", srv.URL, worker))
		var pr scheduler.PollResponse
		decode(t, pollResp, &pr)
		if !pr.HasWork || pr.Assignment == nil {
			return
		}
		id := pr.Assignment.TaskID
		postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/start", srv.URL, id), scheduler.StartTaskRequest{WorkerID: worker}).Body.Close()
		postJSON(t, fmt.Sprintf("%s/v1/tasks/%s/result", srv.URL, id), scheduler.ResultRequest{WorkerID: worker, ImagesProcessed: 1}).Body.Close()
	}
}

func admissionStats(t *testing.T, srv *httptest.Server) (inFlight, admitted, rejected int) {
	t.Helper()
	resp, _ := http.Get(srv.URL + "/v1/metrics/admission")
	var m map[string]any
	decode(t, resp, &m)
	return int(m["in_flight"].(float64)), int(m["admitted_total"].(float64)), int(m["rejected_total"].(float64))
}
