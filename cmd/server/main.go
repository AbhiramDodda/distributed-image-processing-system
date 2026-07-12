package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metadata"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/registry"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/tiering"
)

func main() {
	configPath := flag.String("config", "configs/server.yaml", "config file path")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	idx, err := metadata.Open(cfg.Server.MetadataDBPath)
	if err != nil {
		log.Error("open metadata index", "err", err)
		os.Exit(1)
	}
	defer idx.Close()

	reg, err := registry.Open(cfg.Server.RegistryDBPath)
	if err != nil {
		log.Error("open algorithm registry", "err", err)
		os.Exit(1)
	}
	defer reg.Close()

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

	tierEngine := tiering.New(store, idx, tiering.Policy{
		HotDays:  cfg.Tiering.HotDaysThreshold,
		WarmDays: cfg.Tiering.WarmDaysThreshold,
		ColdDays: cfg.Tiering.ColdDaysThreshold,
	}, log)

	mux := http.NewServeMux()
	registerRoutes(mux, idx, store, tierEngine, log)
	algorithmRoutes(mux, reg, registry.DefaultQuota(), log)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
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
		log.Info("server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

func registerRoutes(mux *http.ServeMux, idx *metadata.Index, store *storage.Client, tier *tiering.Engine, log *slog.Logger) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		dataset := r.URL.Query().Get("dataset")
		if dataset == "" {
			dataset = "train"
		}
		stats, err := idx.DatasetStats(r.Context(), dataset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, stats)
	})

	mux.HandleFunc("/v1/shards", func(w http.ResponseWriter, r *http.Request) {
		dataset := r.URL.Query().Get("dataset")
		if dataset == "" {
			dataset = "train"
		}
		stats, err := idx.ShardStats(r.Context(), dataset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, stats)
	})

	mux.HandleFunc("/v1/shards/", func(w http.ResponseWriter, r *http.Request) {
		// /v1/shards/{shard}/manifest
		path := r.URL.Path[len("/v1/shards/"):]
		parts := splitPath(path)
		if len(parts) != 2 || parts[1] != "manifest" {
			writeError(w, http.StatusBadRequest, "use /v1/shards/{shard}/manifest")
			return
		}
		shard := parts[0]
		dataset := r.URL.Query().Get("dataset")
		if dataset == "" {
			dataset = "train"
		}
		manifest, err := idx.GetShardManifest(r.Context(), shard, dataset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	})

	mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
		label := r.URL.Query().Get("label")
		dataset := r.URL.Query().Get("dataset")
		limitStr := r.URL.Query().Get("limit")
		limit := 100
		if limitStr != "" {
			if n, err := strconv.Atoi(limitStr); err == nil {
				limit = n
			}
		}
		records, err := idx.SearchByLabel(r.Context(), label, dataset, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, records)
	})

	mux.HandleFunc("/v1/labels", func(w http.ResponseWriter, r *http.Request) {
		dataset := r.URL.Query().Get("dataset")
		if dataset == "" {
			dataset = "train"
		}
		counts, err := idx.LabelCounts(r.Context(), dataset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, counts)
	})

	mux.HandleFunc("/v1/tiering/estimate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		var input map[storage.StorageTier]int64
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, tiering.CostProjection(input))
	})

	_ = log
	_ = store
	_ = tier
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func splitPath(path string) []string {
	var parts []string
	start := 0
	for i, c := range path {
		if c == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}
