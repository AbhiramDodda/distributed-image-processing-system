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

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/admission"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/auth"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus/raftgrpc"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/coordinator"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/diag"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metrics"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/ratelimit"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/rpc"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/rpc/coordinatorpb"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
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

	// Opt-in runtime concurrency diagnostics (lock contention/order, invariant
	// checks). Off unless PETABYTE_DIAG is truthy; served at /debug/diag.
	if diag.EnableFromEnv(log) {
		log.Info("concurrency diagnostics active", "endpoint", "/debug/diag")
	}

	coord := coordinator.New(cfg, log)

	// Backpressure: a bounded admission controller sheds job submissions past the
	// platform's stable operating point (HTTP 429) instead of queuing them
	// unboundedly. Off unless max_in_flight_jobs is configured. A single "default"
	// tenant gets the whole capacity here; the multi-tenant API sets per-tenant
	// classes so no tenant's burst can starve another.
	if cfg.Coordinator.MaxInFlightJobs > 0 {
		ctrl := admission.New(admission.Config{MaxInFlight: cfg.Coordinator.MaxInFlightJobs})
		ctrl.SetClass("default", admission.Class{Weight: 1})
		coord.EnableAdmission(ctrl)
		log.Info("admission control active", "max_in_flight_jobs", cfg.Coordinator.MaxInFlightJobs)
	}

	// Raft consensus: route terminal commits through a replicated log so the
	// exactly-once commit decision is agreed across coordinators and survives
	// failover (design.md §3.1). Off unless a raft cluster is configured.
	var raftStop func()
	if cfg.Coordinator.Raft.Enabled() {
		node, fsm, stop, err := startRaft(cfg, log)
		if err != nil {
			log.Error("start raft", "err", err)
			os.Exit(1)
		}
		coord.EnableRaftCommit(node, fsm)
		raftStop = stop
	}

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
	if raftStop != nil {
		raftStop()
	}
	coord.Stop()
}

// startRaft brings up this coordinator's Raft node with the replicated commit
// FSM and a gRPC transport for peer traffic, plus a listener that feeds inbound
// peer messages into the node. It returns the node and FSM (for
// EnableRaftCommit) and a stop function that tears the whole thing down in order.
func startRaft(cfg *config.Config, log *slog.Logger) (*consensus.Node, *consensus.CommitFSM, func(), error) {
	rc := cfg.Coordinator.Raft
	peers := make(map[uint64]string, len(rc.Peers))
	ids := make([]uint64, 0, len(rc.Peers))
	for _, p := range rc.Peers {
		peers[p.ID] = p.Address
		ids = append(ids, p.ID)
	}

	transport := raftgrpc.NewTransport(rc.ID, peers, log)
	fsm := consensus.NewCommitFSM()
	node, err := consensus.Start(consensus.Config{
		ID: rc.ID,
		Peers: ids,
		FSM: fsm,
		Transport: transport,
		Logger: log,
	}, nil)
	if err != nil {
		transport.Close()
		return nil, nil, nil, fmt.Errorf("start raft node: %w", err)
	}

	grpcSrv := grpc.NewServer()
	raftgrpc.RegisterRaftTransportServer(grpcSrv, raftgrpc.NewServer(node))
	addr := fmt.Sprintf("%s:%d", cfg.Coordinator.Host, rc.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		node.Stop()
		transport.Close()
		return nil, nil, nil, fmt.Errorf("listen raft %s: %w", addr, err)
	}
	go func() {
		log.Info("coordinator raft listening", "addr", addr, "id", rc.ID)
		if err := grpcSrv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Error("raft serve", "err", err)
		}
	}()

	stop := func() {
		grpcSrv.GracefulStop()
		node.Stop()
		transport.Close()
	}
	return node, fsm, stop, nil
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
