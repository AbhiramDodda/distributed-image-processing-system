package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/storage"
	"github.com/abhiramd/petabyte-platform/internal/worker"
)

func main() {
	configPath := flag.String("config", "configs/worker.yaml", "config file path")
	workerID := flag.String("id", "", "worker ID (overrides config)")
	port := flag.Int("port", 0, "listen port (overrides config)")
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
