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

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
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
		id: id,
		cfg: cfg,
		log: log,
		store: store,
		coordinator: cfg.Worker.CoordinatorURL,
		sem: make(chan struct{}, cfg.Worker.Concurrency),
		activeTasks: make(map[string]*scheduler.Task),
		stopCh: make(chan struct{}),
	}
}

func (w *Worker) ID() string { return w.id }
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
		ID: w.id,
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
		WorkerID: w.id,
		ImagesProcessed: processed,
		BytesRead: bytesRead,
		OutputKey: outputKey,
		Duration: time.Since(start),
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
		WorkerID: w.id,
		ImagesProcessed: processed,
		BytesRead: bytesRead,
		OutputKey: outputKey,
		Duration: time.Since(start),
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
	// S3 lists keys in lexicographic order, so every worker that lists this shard
	// sees the same sorted slice; the scheduler's range offsets index into it.
	total := int64(len(keys))

	// The worker owns [RangeStart, RangeEnd) but may only touch up to Bound before
	// renewing. end caps slice access; RangeEnd is -1 for an as-yet-unsplit whole
	// shard, in which case the whole listing is ours until proven otherwise.
	start := a.RangeStart
	end := total
	if a.RangeEnd >= 0 && a.RangeEnd < end {
		end = a.RangeEnd
	}
	bound := a.Bound
	gen := a.Generation

	var totalBytes int64
	i := start
	for i < end {
		// Reached the leased bound: renew to extend it (and, on the first call,
		// report the shard size so the scheduler can make this task splittable). If
		// the bound doesn't advance, our tail was reclaimed or the range is spent --
		// stop cooperatively rather than running past what we're leased to touch.
		if i >= bound {
			renewal, err := w.renew(a.TaskID, scheduler.RenewLeaseRequest{
				WorkerID: w.id,
				Generation: gen,
				Frontier: i,
				Total: total,
			})
			if err != nil {
				w.log.Warn("renew lease failed", "task_id", a.TaskID, "err", err)
				break
			}
			gen, bound = renewal.Generation, renewal.Bound
			if renewal.Stolen {
				w.log.Info("lease tail stolen; winding down", "task_id", a.TaskID, "frontier", i, "bound", bound)
			}
			if bound <= i {
				break
			}
		}
		rc, size, err := w.store.Get(ctx, keys[i])
		if err != nil {
			w.log.Warn("get object failed", "key", keys[i], "err", err)
			i++
			continue
		}
		io.Copy(io.Discard, rc)
		rc.Close()
		totalBytes += size
		i++
	}
	processed := i - start

	stagingKey := scheduler.StagingResultKey(a.JobID, a.TaskID)
	result := scheduler.TaskResult{
		TaskID: a.TaskID,
		JobID: a.JobID,
		WorkerID: w.id,
		ImagesProcessed: processed,
		BytesRead: totalBytes,
		OutputKey: scheduler.FinalResultKey(a.JobID, a.Shard, scheduler.Range{Start: a.RangeStart, End: a.RangeEnd, Split: a.Split}),
	}
	body, err := json.Marshal(result)
	if err != nil {
		return 0, 0, "", fmt.Errorf("marshal result: %w", err)
	}
	if err := w.store.Put(ctx, stagingKey, bytes.NewReader(body), "application/json"); err != nil {
		return 0, 0, "", fmt.Errorf("stage result %s: %w", stagingKey, err)
	}
	return processed, totalBytes, stagingKey, nil
}

// renew reports progress on a task and returns the extended lease. It is the
// worker half of the work-stealing protocol: by only advancing up to the bound
// this returns, the worker guarantees the scheduler that the un-granted tail is
// untouched and therefore safe to hand to an idle worker.
func (w *Worker) renew(taskID string, req scheduler.RenewLeaseRequest) (scheduler.LeaseRenewal, error) {
	var renewal scheduler.LeaseRenewal
	err := postJSONResp(w.coordinator+"/v1/tasks/"+taskID+"/renew", req, &renewal)
	return renewal, err
}
