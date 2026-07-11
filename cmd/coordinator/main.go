package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/abhiramd/petabyte-platform/internal/auth"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/coordinator"
	"github.com/abhiramd/petabyte-platform/internal/metrics"
	"github.com/abhiramd/petabyte-platform/internal/ratelimit"
	"github.com/abhiramd/petabyte-platform/internal/rpc"
	"github.com/abhiramd/petabyte-platform/internal/rpc/coordinatorpb"
	"github.com/abhiramd/petabyte-platform/internal/storage"
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

	// A configured bucket enables the exactly-once result-commit path: the
	// coordinator promotes each worker's staged output to its canonical key.
	// Leaving the bucket empty runs the coordinator storage-free (at-least-once).
	if cfg.Storage.Bucket != "" {
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
		coord.EnableResultCommit(store)
	}

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

	grpcSrv, err := startGRPC(cfg, coord, log)
	if err != nil {
		log.Error("start grpc", "err", err)
		os.Exit(1)
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
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	coord.Stop()
}

// startGRPC brings up the Level 6 gRPC control-plane API when a grpc_port is
// configured, guarded by the auth/RBAC/rate-limit interceptor chain. It returns a
// nil server (and no error) when gRPC is disabled, so the coordinator runs
// HTTP-only by default. The scheduler is served directly -- it satisfies
// rpc.JobService.
func startGRPC(cfg *config.Config, coord *coordinator.Coordinator, log *slog.Logger) (*grpc.Server, error) {
	cc := cfg.Coordinator
	if cc.GRPCPort <= 0 {
		return nil, nil
	}
	if cc.JWTSecret == "" {
		return nil, fmt.Errorf("grpc_port set but jwt_secret is empty; refusing to serve an unauthenticated API")
	}

	verifier := auth.NewVerifier([]byte(cc.JWTSecret), 30*time.Second)
	limiter := ratelimit.New(cc.RateLimitPerSec, cc.RateLimitBurst)
	ic := rpc.NewAuthInterceptor(verifier, auth.DefaultPolicy(), limiter)

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(ic.Unary),
		grpc.StreamInterceptor(ic.Stream),
	)
	coordinatorpb.RegisterCoordinatorServer(grpcSrv, rpc.NewServer(coord.Scheduler(), 0))

	grpcAddr := fmt.Sprintf("%s:%d", cc.Host, cc.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("listen grpc %s: %w", grpcAddr, err)
	}
	go func() {
		log.Info("coordinator grpc listening", "addr", grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Error("grpc serve", "err", err)
		}
	}()
	return grpcSrv, nil
}
