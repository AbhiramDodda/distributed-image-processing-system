// Command sandbox-runner is the sidecar that runs inside a worker pod and
// executes exactly one shard task inside a gVisor sandbox. The coordinator/
// operator injects the task assignment and algorithm image via env vars; the
// runner stages the shard, runs the untrusted image with no network, collects
// results, and reports back. It is deliberately single-shot: the pod exits when
// the task finishes, and K8s Job semantics handle retries.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/sandbox"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

func main() {
	configPath := flag.String("config", "configs/worker.yaml", "config file path")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	assignment, err := decodeAssignment(os.Getenv("PETABYTE_TASK_JSON"))
	if err != nil {
		log.Error("decode task assignment", "err", err)
		os.Exit(1)
	}
	image := os.Getenv("PETABYTE_ALGORITHM_IMAGE")
	if image == "" {
		log.Error("PETABYTE_ALGORITHM_IMAGE not set")
		os.Exit(1)
	}
	coordinator := envOr("PETABYTE_COORDINATOR_URL", cfg.Worker.CoordinatorURL)
	workerID := envOr("PETABYTE_WORKER_ID", cfg.Worker.ID)

	ctx := context.Background()
	store, err := storage.NewClient(ctx, storage.ClientConfig{
		Endpoint: cfg.Storage.Endpoint,
		Region: cfg.Storage.Region,
		Bucket: cfg.Storage.Bucket,
		AccessKeyID: cfg.Storage.AccessKeyID,
		SecretAccessKey: cfg.Storage.SecretAccessKey,
		UsePathStyle: cfg.Storage.UsePathStyle,
	})
	if err != nil {
		log.Error("init storage", "err", err)
		os.Exit(1)
	}

	scratch, err := os.MkdirTemp("", "petabyte-task-*")
	if err != nil {
		log.Error("scratch dir", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(scratch)

	runner := sandbox.Runner{
		Runtime: sandbox.RunscRuntime{},
		Store: store,
	}
	spec := sandbox.RunnerSpec{
		TaskID: assignment.TaskID,
		JobID: assignment.JobID,
		Shard: assignment.Shard,
		Dataset: assignment.Dataset,
		Image: image,
		Limits: limitsFromEnv(),
		Env: map[string]string{
			"PETABYTE_SHARD": assignment.Shard,
			"PETABYTE_DATASET": assignment.Dataset,
		},
	}

	postJSON(coordinator+"/v1/tasks/"+assignment.TaskID+"/start", scheduler.StartTaskRequest{WorkerID: workerID}, log)
	start := time.Now()
	result, execErr := runner.Execute(ctx, spec, scratch)

	req := scheduler.ResultRequest{WorkerID: workerID, Duration: time.Since(start)}
	if result != nil {
		req.ImagesProcessed = result.ImagesProcessed
		req.BytesRead = result.BytesRead
		req.OutputKey = result.OutputKey
	}
	if execErr != nil {
		req.Error = execErr.Error()
		log.Error("task failed", "task_id", assignment.TaskID, "err", execErr)
	} else {
		log.Info("task done", "task_id", assignment.TaskID, "images", req.ImagesProcessed)
	}
	postJSON(coordinator+"/v1/tasks/"+assignment.TaskID+"/result", req, log)

	if execErr != nil {
		os.Exit(1)
	}
}

func decodeAssignment(encoded string) (*scheduler.TaskAssignment, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	var a scheduler.TaskAssignment
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, err
	}
	return &a, nil
}

// limitsFromEnv reads the resource caps the operator stamped onto the pod.
// Network is always "none"; users cannot opt out of the network sandbox.
func limitsFromEnv() sandbox.ResourceLimits {
	return sandbox.ResourceLimits{
		CPUCores: envFloat("PETABYTE_CPU_CORES", 1),
		MemoryBytes: int64(envInt("PETABYTE_MEMORY_GB", 2)) << 30,
		GPUCount: envInt("PETABYTE_GPU_COUNT", 0),
		DiskBytes: 10 << 30,
		NetworkMode: "none",
		TimeoutSec: envInt("PETABYTE_TIMEOUT_SEC", 3600),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return fallback
}

func postJSON(url string, v any, log *slog.Logger) {
	body, err := json.Marshal(v)
	if err != nil {
		log.Warn("marshal report", "err", err)
		return
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Warn("report to coordinator", "url", url, "err", err)
		return
	}
	resp.Body.Close()
}
