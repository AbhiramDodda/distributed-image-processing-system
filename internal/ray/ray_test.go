package ray_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/ray"
)

func TestHealthCheck_success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster_info" {
			t.Errorf("path = %q, want /api/cluster_info", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"node_count": 4,
				"gpu_count": 8,
				"ray_version": "2.9.0",
			},
		})
	}))
	defer srv.Close()

	info, err := ray.NewClient(srv.URL).HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if info.NodeCount != 4 {
		t.Errorf("NodeCount = %d, want 4", info.NodeCount)
	}
	if info.GPUCount != 8 {
		t.Errorf("GPUCount = %d, want 8", info.GPUCount)
	}
	if info.RayVersion != "2.9.0" {
		t.Errorf("RayVersion = %q, want 2.9.0", info.RayVersion)
	}
}

func TestHealthCheck_unreachable(t *testing.T) {
	// Point at a closed server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	if _, err := ray.NewClient(srv.URL).HealthCheck(context.Background()); err == nil {
		t.Fatal("HealthCheck against closed server should error")
	}
}

func TestHealthCheck_errorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if _, err := ray.NewClient(srv.URL).HealthCheck(context.Background()); err == nil {
		t.Fatal("HealthCheck should error on 503")
	}
}

func TestSubmitJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/jobs/" {
			t.Errorf("path = %q, want /api/jobs/", r.URL.Path)
		}
		var req ray.SubmitRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.Entrypoint == "" {
			t.Error("entrypoint should be set")
		}
		json.NewEncoder(w).Encode(map[string]string{"submission_id": "sub-123"})
	}))
	defer srv.Close()

	id, err := ray.NewClient(srv.URL).SubmitJob(context.Background(), ray.SubmitRequest{
		Entrypoint: "python main.py",
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if id != "sub-123" {
		t.Errorf("submission id = %q, want sub-123", id)
	}
}

func TestGetJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"submission_id": "sub-123",
			"status": "SUCCEEDED",
		})
	}))
	defer srv.Close()

	info, err := ray.NewClient(srv.URL).GetJob(context.Background(), "sub-123")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if info.Status != ray.JobStatusSucceeded {
		t.Errorf("status = %q, want SUCCEEDED", info.Status)
	}
}

func TestWaitForJob_terminalState(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		status := "RUNNING"
		if calls >= 3 {
			status = "SUCCEEDED"
		}
		json.NewEncoder(w).Encode(map[string]any{
			"submission_id": "sub-123",
			"status": status,
		})
	}))
	defer srv.Close()

	info, err := ray.NewClient(srv.URL).WaitForJob(context.Background(), "sub-123", time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForJob: %v", err)
	}
	if info.Status != ray.JobStatusSucceeded {
		t.Errorf("final status = %q, want SUCCEEDED", info.Status)
	}
	if calls < 3 {
		t.Errorf("expected at least 3 polls, got %d", calls)
	}
}

func TestWaitForJob_contextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"submission_id": "sub-123",
			"status": "RUNNING", // never terminal
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if _, err := ray.NewClient(srv.URL).WaitForJob(ctx, "sub-123", time.Millisecond); err == nil {
		t.Fatal("WaitForJob should return ctx error when job never finishes")
	}
}
