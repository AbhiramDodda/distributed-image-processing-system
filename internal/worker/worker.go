package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
	"github.com/abhiramd/petabyte-platform/internal/storage"
)

type Worker struct {
	id string
	cfg *config.Config
	log *slog.Logger
	store *storage.Client
	coordinator string
	sem chan struct{}
	activeTasks map[string]*scheduler.Task
	mu sync.RWMutex
	stopCh chan struct{}
}

func New(cfg *config.Config, store *storage.Client, log *slog.Logger) *Worker {
	id := cfg.Worker.ID
	if id == "" {
		id = uuid.New().String()
	}
	return &Worker{
		id:          id,
		cfg:         cfg,
		log:         log,
		store:       store,
		coordinator: cfg.Worker.CoordinatorURL,
		sem:         make(chan struct{}, cfg.Worker.Concurrency),
		activeTasks: make(map[string]*scheduler.Task),
		stopCh:      make(chan struct{}),
	}
}

func (w *Worker) ID() string      { return w.id }
func (w *Worker) Address() string { return fmt.Sprintf("%s:%d", w.cfg.Worker.Host, w.cfg.Worker.Port) }

func (w *Worker) Start(ctx context.Context) error {
	if err := w.register(); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	w.log.Info("registered with coordinator", "coordinator", w.coordinator, "id", w.id)
	go w.pollLoop(ctx)
	go w.heartbeatLoop(ctx)
	return nil
}

func (w *Worker) Stop() { close(w.stopCh) }

func (w *Worker) ActiveTaskCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.activeTasks)
}

func (w *Worker) register() error {
	return postJSON(w.coordinator+"/v1/cluster/register", cluster.RegisterRequest{
		ID:      w.id,
		Address: w.Address(),
	})
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Worker.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.sendHeartbeat()
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) sendHeartbeat() {
	err := postJSON(w.coordinator+"/v1/cluster/heartbeat", cluster.Heartbeat{
		NodeID: w.id,
		Metrics: cluster.NodeMetrics{ActiveTasks: w.ActiveTaskCount()},
	})
	if err != nil {
		w.log.Warn("heartbeat failed", "err", err)
	}
}

func (w *Worker) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Worker.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if w.ActiveTaskCount() < w.cfg.Worker.Concurrency {
				w.poll(ctx)
			}
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) poll(ctx context.Context) {
	resp, err := http.Get(fmt.Sprintf("%s/v1/tasks/poll?worker=%s", w.coordinator, w.id))
	if err != nil {
		w.log.Warn("poll failed", "err", err)
		return
	}
	defer resp.Body.Close()
	var pr scheduler.PollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		w.log.Warn("decode poll response", "err", err)
		return
	}
	if !pr.HasWork || pr.Assignment == nil {
		return
	}
	go w.executeTask(ctx, *pr.Assignment)
}

func (w *Worker) executeTask(ctx context.Context, a scheduler.TaskAssignment) {
	w.sem <- struct{}{}
	defer func() { <-w.sem }()

	w.log.Info("task started", "task_id", a.TaskID, "shard", a.Shard, "dataset", a.Dataset)
	postJSON(w.coordinator+"/v1/tasks/"+a.TaskID+"/start", scheduler.StartTaskRequest{WorkerID: w.id})

	start := time.Now()
	processed, bytesRead, outputKey, execErr := w.runAlgorithm(ctx, a)

	req := scheduler.ResultRequest{
		WorkerID:        w.id,
		ImagesProcessed: processed,
		BytesRead:       bytesRead,
		OutputKey:       outputKey,
		Duration:        time.Since(start),
	}
	if execErr != nil {
		req.Error = execErr.Error()
		w.log.Error("task failed", "task_id", a.TaskID, "err", execErr)
	} else {
		w.log.Info("task done", "task_id", a.TaskID, "images", processed, "bytes", bytesRead)
	}

	if err := postJSON(w.coordinator+"/v1/tasks/"+a.TaskID+"/result", req); err != nil {
		w.log.Error("report result failed", "task_id", a.TaskID, "err", err)
	}
}

// RunTask executes a single TaskAssignment and reports the result to the
// coordinator. Used by K8s Job pods that receive their assignment via env var.
func (w *Worker) RunTask(ctx context.Context, a scheduler.TaskAssignment) error {
	w.log.Info("single-task mode", "task_id", a.TaskID, "shard", a.Shard, "dataset", a.Dataset)
	postJSON(w.coordinator+"/v1/tasks/"+a.TaskID+"/start", scheduler.StartTaskRequest{WorkerID: w.id})
	start := time.Now()
	processed, bytesRead, outputKey, execErr := w.runAlgorithm(ctx, a)
	req := scheduler.ResultRequest{
		WorkerID:        w.id,
		ImagesProcessed: processed,
		BytesRead:       bytesRead,
		OutputKey:       outputKey,
		Duration:        time.Since(start),
	}
	if execErr != nil {
		req.Error = execErr.Error()
		w.log.Error("task failed", "task_id", a.TaskID, "err", execErr)
	} else {
		w.log.Info("task done", "task_id", a.TaskID, "images", processed, "bytes", bytesRead)
	}
	return postJSON(w.coordinator+"/v1/tasks/"+a.TaskID+"/result", req)
}

// runAlgorithm is the Level 2 placeholder. Level 4 replaces this with
// sandboxed container execution via gVisor.
//
// It writes its output to a *staging* key rather than the task's final location.
// The staged object is invisible to consumers until the coordinator commits it
// (server-side copy to the canonical key) as it marks the task done. That two-
// phase split is what upgrades the pipeline from at-least-once to effectively
// exactly-once: a task that is re-run after a crash or false-positive failure
// stages a fresh object, but only the coordinator's commit makes any of them the
// one visible result. See scheduler.StagingResultKey / FinalResultKey.
func (w *Worker) runAlgorithm(ctx context.Context, a scheduler.TaskAssignment) (int64, int64, string, error) {
	prefix := fmt.Sprintf("%s/%s/", a.Dataset, a.Shard)
	keys, err := w.store.ListPrefix(ctx, prefix)
	if err != nil {
		return 0, 0, "", fmt.Errorf("list shard %s: %w", a.Shard, err)
	}
	var totalBytes int64
	for _, key := range keys {
		rc, size, err := w.store.Get(ctx, key)
		if err != nil {
			w.log.Warn("get object failed", "key", key, "err", err)
			continue
		}
		io.Copy(io.Discard, rc)
		rc.Close()
		totalBytes += size
	}

	stagingKey := scheduler.StagingResultKey(a.JobID, a.TaskID)
	result := scheduler.TaskResult{
		TaskID:          a.TaskID,
		JobID:           a.JobID,
		WorkerID:        w.id,
		ImagesProcessed: int64(len(keys)),
		BytesRead:       totalBytes,
		OutputKey:       scheduler.FinalResultKey(a.JobID, a.Shard, scheduler.Range{Start: a.RangeStart, End: a.RangeEnd, Split: a.Split}),
	}
	body, err := json.Marshal(result)
	if err != nil {
		return 0, 0, "", fmt.Errorf("marshal result: %w", err)
	}
	if err := w.store.Put(ctx, stagingKey, bytes.NewReader(body), "application/json"); err != nil {
		return 0, 0, "", fmt.Errorf("stage result %s: %w", stagingKey, err)
	}
	return int64(len(keys)), totalBytes, stagingKey, nil
}
