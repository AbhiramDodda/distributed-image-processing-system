package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
	"github.com/abhiramd/petabyte-platform/internal/storage"
	"github.com/abhiramd/petabyte-platform/internal/worker"
)

func main() {
	configPath := flag.String("config", "configs/worker.yaml", "config file path")
	workerID := flag.String("id", "", "worker ID (overrides config)")
	port := flag.Int("port", 0, "listen port (overrides config)")
	singleTask := flag.Bool("single-task", false, "run one task from PETABYTE_TASK_JSON env var and exit (K8s Job mode)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}
	if *workerID != "" {
		cfg.Worker.ID = *workerID
	}
	if *port > 0 {
		cfg.Worker.Port = *port
	}
	if coordinatorURL := os.Getenv("PETABYTE_COORDINATOR_URL"); coordinatorURL != "" {
		cfg.Worker.CoordinatorURL = coordinatorURL
	}

	store, err := storage.NewClient(context.Background(), storage.ClientConfig{
		Endpoint:        cfg.Storage.Endpoint,
		Region:          cfg.Storage.Region,
		Bucket:          cfg.Storage.Bucket,
		AccessKeyID:     cfg.Storage.AccessKeyID,
		SecretAccessKey: cfg.Storage.SecretAccessKey,
		UsePathStyle:    cfg.Storage.UsePathStyle,
	})
	if err != nil {
		log.Error("init storage", "err", err)
		os.Exit(1)
	}

	w := worker.New(cfg, store, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// K8s Job mode: execute a single task received via env var and exit.
	if *singleTask || os.Getenv("PETABYTE_TASK_JSON") != "" {
		encoded := os.Getenv("PETABYTE_TASK_JSON")
		if encoded == "" {
			log.Error("PETABYTE_TASK_JSON not set in single-task mode")
			os.Exit(1)
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			log.Error("decode PETABYTE_TASK_JSON", "err", err)
			os.Exit(1)
		}
		var a scheduler.TaskAssignment
		if err := json.Unmarshal(raw, &a); err != nil {
			log.Error("unmarshal task assignment", "err", err)
			os.Exit(1)
		}
		if err := w.RunTask(ctx, a); err != nil {
			log.Error("task failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := w.Start(ctx); err != nil {
		log.Error("start worker", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	worker.NewAPI(w).Register(mux)

	addr := fmt.Sprintf("%s:%d", cfg.Worker.Host, cfg.Worker.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 300 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Info("worker listening", "addr", addr, "id", w.ID())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	log.Info("shutting down worker")
	cancel()
	w.Stop()
	shutCtx, sc := context.WithTimeout(context.Background(), 15*time.Second)
	defer sc()
	srv.Shutdown(shutCtx)
}
