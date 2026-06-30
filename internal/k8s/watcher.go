package k8s

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type JobPhase string

const (
	JobSucceeded JobPhase = "Succeeded"
	JobFailed    JobPhase = "Failed"
)

type jobStatus struct {
	Succeeded int32 `json:"succeeded"`
	Failed    int32 `json:"failed"`
	Active    int32 `json:"active"`
	Conditions []struct {
		Type   string `json:"type"`
		Status string `json:"status"`
	} `json:"conditions,omitempty"`
}

type jobItem struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Status jobStatus `json:"status"`
}

type jobList struct {
	Items []jobItem `json:"items"`
}

// JobResult is sent to the coordinator when a K8s Job completes.
type JobResult struct {
	TaskID  string
	JobName string
	Phase   JobPhase
	Error   string
}

// Watcher polls K8s for petabyte worker Jobs and notifies results.
type Watcher struct {
	client   *Client
	interval time.Duration
	results  chan JobResult
	log      *slog.Logger
}

func NewWatcher(client *Client, interval time.Duration, log *slog.Logger) *Watcher {
	return &Watcher{
		client:   client,
		interval: interval,
		results:  make(chan JobResult, 256),
		log:      log,
	}
}

func (w *Watcher) Results() <-chan JobResult { return w.results }

// Run polls K8s every interval, emitting completed/failed Jobs.
// Completed Jobs are deleted after their result is emitted to avoid re-emission.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.poll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Watcher) poll(ctx context.Context) {
	list, err := w.listWorkerJobs(ctx)
	if err != nil {
		w.log.Warn("list jobs failed", "err", err)
		return
	}
	for _, item := range list.Items {
		phase, errMsg := resolvePhase(item.Status)
		if phase == "" {
			continue
		}
		taskID := item.Metadata.Labels["task-id"]
		if taskID == "" {
			continue
		}
		w.results <- JobResult{
			TaskID:  taskID,
			JobName: item.Metadata.Name,
			Phase:   phase,
			Error:   errMsg,
		}
		if err := w.deleteJob(ctx, item.Metadata.Name); err != nil {
			w.log.Warn("delete completed job failed", "name", item.Metadata.Name, "err", err)
		}
	}
}

func resolvePhase(s jobStatus) (JobPhase, string) {
	if s.Succeeded > 0 {
		return JobSucceeded, ""
	}
	if s.Failed > 0 {
		for _, c := range s.Conditions {
			if c.Type == "Failed" && c.Status == "True" {
				return JobFailed, "job backoff limit reached"
			}
		}
		return JobFailed, "job failed"
	}
	return "", ""
}

func (w *Watcher) listWorkerJobs(ctx context.Context) (*jobList, error) {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs?labelSelector=app%%3Dpetabyte-worker", w.client.namespace)
	req, err := w.client.newRequest(http.MethodGet, path)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	resp, err := w.client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("k8s list jobs: status %d", resp.StatusCode)
	}
	var list jobList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode job list: %w", err)
	}
	return &list, nil
}

func (w *Watcher) deleteJob(ctx context.Context, name string) error {
	// foreground deletion propagates to pods
	body := []byte(`{"propagationPolicy":"Foreground"}`)
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s", w.client.namespace, name)
	req, err := w.client.newRequest(http.MethodDelete, path)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := w.client.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
