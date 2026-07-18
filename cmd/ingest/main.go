package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/ingestion"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metadata"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

func main() {
	configPath := flag.String("config", "configs/server.yaml", "config file path")
	dir := flag.String("dir", "", "local directory to ingest (required)")
	dataset := flag.String("dataset", "train", "dataset name in the platform")
	labels := flag.String("labels", "", "comma-separated labels to tag all images")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "error: -dir is required")
		flag.Usage()
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
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

	idx, err := metadata.Open(cfg.Server.MetadataDBPath)
	if err != nil {
		log.Error("open metadata index", "err", err)
		os.Exit(1)
	}
	defer idx.Close()

	var tagList []string
	if *labels != "" {
		for _, l := range strings.Split(*labels, ",") {
			if t := strings.TrimSpace(l); t != "" {
				tagList = append(tagList, t)
			}
		}
	}

	pipeline := ingestion.New(store, idx, cfg.Ingestion.Workers, log)
	pipeline.WithMultipart(
		cfg.Storage.MultipartThreshold,
		cfg.Storage.MultipartChunkSize,
		cfg.Storage.MultipartConcurrency,
	)

	log.Info("ingestion started", "dir", *dir, "dataset", *dataset, "workers", cfg.Ingestion.Workers)
	start := time.Now()

	progress, err := pipeline.IngestDir(context.Background(), *dir, *dataset, tagList)
	if err != nil {
		log.Error("ingest error", "err", err)
	}

	elapsed := time.Since(start)
	log.Info("ingestion complete",
		"processed", progress.Processed,
		"failed", progress.Failed,
		"bytes", progress.Bytes,
		"elapsed", elapsed,
		"throughput_img_s", float64(progress.Processed)/elapsed.Seconds(),
	)

	// A handful of transient failures in a bulk ingest (e.g. an occasional
	// SQLITE_BUSY under heavy write concurrency over millions of objects) must
	// not fail the whole run. Only exit nonzero if nothing landed or failures
	// exceed 1% of the attempted objects.
	if progress.Processed == 0 ||
		float64(progress.Failed) > 0.01*float64(progress.Processed+progress.Failed) {
		os.Exit(1)
	}
}
