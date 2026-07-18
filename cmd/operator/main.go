package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/k8s"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

func main() {
	configPath := flag.String("config", "configs/operator.yaml", "config file path")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	kube, err := k8s.NewClient(cfg.Operator.Namespace, cfg.Operator.KubeconfigPath)
	if err != nil {
		log.Error("init k8s client", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher := k8s.NewWatcher(kube, 10*time.Second, log)
	go watcher.Run(ctx)
	go reportResults(ctx, cfg.Operator.CoordinatorURL, watcher.Results(), log)

	ticker := time.NewTicker(cfg.Operator.PollInterval)
	defer ticker.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Info("operator started",
		"namespace", cfg.Operator.Namespace,
		"coordinator", cfg.Operator.CoordinatorURL,
		"image", cfg.Operator.WorkerImage,
	)

	for {
		select {
		case <-ticker.C:
			if err := cycle(ctx, cfg, kube, log); err != nil {
				log.Warn("operator cycle error", "err", err)
			}
		case <-quit:
			log.Info("operator shutting down")
			cancel()
			return
		}
	}
}

// cycle drains pending tasks from the coordinator and creates K8s Jobs for them.
func cycle(ctx context.Context, cfg *config.Config, kube *k8s.Client, log *slog.Logger) error {
	assignments, err := drainPending(cfg.Operator.CoordinatorURL, cfg.Operator.MaxJobsPerCycle)
	if err != nil {
		return fmt.Errorf("drain pending: %w", err)
	}
	if len(assignments) == 0 {
		return nil
	}

	nodes, err := activeNodes(cfg.Operator.CoordinatorURL)
	if err != nil {
		log.Warn("could not fetch node list for affinity scheduling", "err", err)
		nodes = nil
	}

	for _, a := range assignments {
		job, err := k8s.BuildJob(a, cfg.Operator.WorkerImage, cfg.Operator.CoordinatorURL, nodes)
		if err != nil {
			log.Error("build job spec failed", "task_id", a.TaskID, "err", err)
			continue
		}
		if err := kube.CreateJob(job); err != nil {
			log.Error("create k8s job failed", "task_id", a.TaskID, "err", err)
			continue
		}
		log.Info("k8s job created", "task_id", a.TaskID, "shard", a.Shard)
	}
	return nil
}

// reportResults listens for completed K8s Jobs and posts results to the coordinator.
func reportResults(ctx context.Context, coordinatorURL string, results <-chan k8s.JobResult, log *slog.Logger) {
	for {
		select {
		case r := <-results:
			errMsg := ""
			if r.Phase == k8s.JobFailed {
				errMsg = r.Error
				if errMsg == "" {
					errMsg = "k8s job failed"
				}
			}
			req := map[string]any{
				"worker_id": "k8s-operator",
				"error": errMsg,
			}
			if err := postJSON(coordinatorURL+"/v1/tasks/"+r.TaskID+"/result", req); err != nil {
				log.Error("report task result failed", "task_id", r.TaskID, "err", err)
			} else {
				log.Info("task result reported", "task_id", r.TaskID, "phase", r.Phase)
			}
		case <-ctx.Done():
			return
		}
	}
}

func drainPending(coordinatorURL string, n int) ([]scheduler.TaskAssignment, error) {
	url := fmt.Sprintf("%s/v1/operator/drain?n=%d", coordinatorURL, n)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var assignments []scheduler.TaskAssignment
	if err := json.NewDecoder(resp.Body).Decode(&assignments); err != nil {
		return nil, fmt.Errorf("decode drain response: %w", err)
	}
	return assignments, nil
}

func activeNodes(coordinatorURL string) ([]*cluster.NodeInfo, error) {
	resp, err := http.Get(coordinatorURL + "/v1/cluster/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var nodes []*cluster.NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	var active []*cluster.NodeInfo
	for _, n := range nodes {
		if n.State == cluster.NodeActive {
			active = append(active, n)
		}
	}
	return active, nil
}

func postJSON(url string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e map[string]string
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server %d: %s", resp.StatusCode, e["error"])
	}
	return nil
}
