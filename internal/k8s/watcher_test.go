package k8s

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

var testLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func TestResolvePhase(t *testing.T) {
	cases := []struct {
		name string
		status jobStatus
		wantPhase JobPhase
		wantErr bool
	}{
		{
			name: "succeeded",
			status: jobStatus{Succeeded: 1},
			wantPhase: JobSucceeded,
		},
		{
			name: "failed with condition",
			status: jobStatus{Failed: 1, Conditions: []struct {
				Type string `json:"type"`
				Status string `json:"status"`
			}{{Type: "Failed", Status: "True"}}},
			wantPhase: JobFailed,
			wantErr: true,
		},
		{
			name: "failed without condition",
			status: jobStatus{Failed: 1},
			wantPhase: JobFailed,
			wantErr: true,
		},
		{
			name: "still active",
			status: jobStatus{Active: 1},
			wantPhase: "",
		},
		{
			name: "not started",
			status: jobStatus{},
			wantPhase: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			phase, errMsg := resolvePhase(c.status)
			if phase != c.wantPhase {
				t.Errorf("phase = %q, want %q", phase, c.wantPhase)
			}
			if c.wantErr && errMsg == "" {
				t.Error("expected non-empty error message for failed job")
			}
			if !c.wantErr && errMsg != "" {
				t.Errorf("unexpected error message: %q", errMsg)
			}
		})
	}
}

// TestWatcher_poll_emitsAndDeletes drives the watcher against a fake K8s API
// server returning one succeeded and one failed Job, and asserts that both
// results are emitted and a DELETE is issued for each.
func TestWatcher_poll_emitsAndDeletes(t *testing.T) {
	const listBody = `{
		"items": [
			{"metadata": {"name": "petabyte-task-aaa", "labels": {"task-id": "task-succeeded"}},
			 "status": {"succeeded": 1}},
			{"metadata": {"name": "petabyte-task-bbb", "labels": {"task-id": "task-failed"}},
			 "status": {"failed": 1, "conditions": [{"type":"Failed","status":"True"}]}},
			{"metadata": {"name": "petabyte-task-ccc", "labels": {"task-id": "task-running"}},
			 "status": {"active": 1}}
		]
	}`

	deleted := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if !strings.Contains(r.URL.RawQuery, "labelSelector") {
				t.Errorf("list request missing labelSelector: %s", r.URL.RawQuery)
			}
			w.Write([]byte(listBody))
		case http.MethodDelete:
			parts := strings.Split(r.URL.Path, "/")
			deleted <- parts[len(parts)-1]
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()

	client := &Client{
		apiserver: srv.URL,
		namespace: "petabyte",
		httpClient: srv.Client(),
	}
	w := NewWatcher(client, time.Hour, testLog)

	go w.poll(context.Background())

	results := make(map[string]JobPhase)
	timeout := time.After(2 * time.Second)
	for len(results) < 2 {
		select {
		case r := <-w.Results():
			results[r.TaskID] = r.Phase
		case <-timeout:
			t.Fatalf("timed out; got %d results, want 2", len(results))
		}
	}

	if results["task-succeeded"] != JobSucceeded {
		t.Errorf("task-succeeded phase = %q, want Succeeded", results["task-succeeded"])
	}
	if results["task-failed"] != JobFailed {
		t.Errorf("task-failed phase = %q, want Failed", results["task-failed"])
	}
	if _, ok := results["task-running"]; ok {
		t.Error("running task should not be emitted as a result")
	}

	// Both terminal jobs must be deleted (running one must not).
	deletedNames := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-deleted:
			deletedNames[name] = true
		case <-time.After(time.Second):
			t.Fatalf("expected 2 deletes, saw %d", len(deletedNames))
		}
	}
	if !deletedNames["petabyte-task-aaa"] || !deletedNames["petabyte-task-bbb"] {
		t.Errorf("wrong jobs deleted: %v", deletedNames)
	}
}
