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
	"github.com/abhiramd/petabyte-platform/internal/coordinator"
	"github.com/abhiramd/petabyte-platform/internal/metrics"
)

func main() {
	configPath := flag.String("config", "configs/coordinator.yaml", "config file path")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	coord := coordinator.New(cfg, log)
	mc := metrics.NewCollector()

	if cfg.Coordinator.WALDir != "" {
		if err := coord.EnablePersistence(cfg.Coordinator.WALDir, cfg.Coordinator.CheckpointInterval); err != nil {
			log.Error("enable persistence", "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coord.Start(ctx)

	mux := http.NewServeMux()
	coordinator.NewAPI(coord).Register(mux)
	if cfg.Metrics.Enabled {
		mux.Handle(cfg.Metrics.Path, mc.Handler())
	}

	addr := fmt.Sprintf("%s:%d", cfg.Coordinator.Host, cfg.Coordinator.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Info("coordinator listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	log.Info("shutting down")
	cancel()
	// Stop accepting requests before Stop() writes the final checkpoint and
	// closes the WAL, so no in-flight handler appends to a closing log.
	shutCtx, sc := context.WithTimeout(context.Background(), 15*time.Second)
	defer sc()
	srv.Shutdown(shutCtx)
	coord.Stop()
}
