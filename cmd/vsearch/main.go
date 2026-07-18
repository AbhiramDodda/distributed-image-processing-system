// Command vsearch runs nearest-neighbour image similarity search over a CLIP
// embedding corpus (a Parquet file produced by scripts/clip/embed_corpus.py).
//
// The index loads from a local path or an object-storage key. A query is one of:
//
//	-id <corpus id>     find images similar to an existing corpus image (no encoder)
//	-vector '[..]'      a raw JSON vector (no encoder; handy for scripting/tests)
//	-text  "a dog"      text->image; encoded live by the CLIP sidecar (-encoder)
//	-image path.jpg     novel image->image; encoded live by the CLIP sidecar
//
//	go run ./cmd/vsearch -index embeddings.parquet -text "a photo of a dog" -k 5
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/vsearch"
)

func main() {
	configPath := flag.String("config", "configs/server.yaml", "config file path (for object-storage credentials when -index is an s3:// key)")
	index := flag.String("index", "embeddings.parquet", "embedding Parquet: local path or s3://<key>")
	k := flag.Int("k", 10, "number of neighbours to return")
	id := flag.String("id", "", "query: an existing corpus id (image->image, no encoder)")
	vector := flag.String("vector", "", "query: a raw JSON vector like '[0.1,-0.2,...]'")
	text := flag.String("text", "", "query: a text prompt (text->image, needs -encoder)")
	imagePath := flag.String("image", "", "query: a local image file (image->image, needs -encoder)")
	encoder := flag.String("encoder", "http://127.0.0.1:8600", "CLIP encode sidecar base URL (for -text/-image)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log, *configPath, *index, *k, *id, *vector, *text, *imagePath, *encoder); err != nil {
		log.Error("vsearch", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, configPath, index string, k int, id, vector, text, imagePath, encoder string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, err := buildQuery(k, id, vector, text, imagePath)
	if err != nil {
		return err
	}

	// Object storage is only needed for an s3:// index; build it lazily and tolerate a
	// missing config so the common local-file case has no dependencies.
	var store vsearch.ObjectGetter
	if cfg, err := config.Load(configPath); err == nil {
		if sc, err := storage.NewClient(ctx, storage.ClientConfig{
			Endpoint:        cfg.Storage.Endpoint,
			Region:          cfg.Storage.Region,
			Bucket:          cfg.Storage.Bucket,
			AccessKeyID:     cfg.Storage.AccessKeyID,
			SecretAccessKey: cfg.Storage.SecretAccessKey,
			UsePathStyle:    cfg.Storage.UsePathStyle,
		}); err == nil {
			store = sc
		}
	}

	ix, err := vsearch.LoadIndex(ctx, store, index)
	if err != nil {
		return err
	}
	log.Info("index loaded", "vectors", ix.Len(), "dim", ix.Dim(), "source", index)

	results, err := ix.Run(ctx, q, vsearch.NewHTTPEncoder(encoder, nil))
	if err != nil {
		return err
	}
	for rank, r := range results {
		fmt.Printf("%2d  %.4f  %s\n", rank+1, r.Score, r.ID)
	}
	return nil
}

// buildQuery turns the mutually-exclusive query flags into a vsearch.Query, failing
// if zero or more than one mode is set.
func buildQuery(k int, id, vector, text, imagePath string) (vsearch.Query, error) {
	q := vsearch.Query{K: k}
	set := 0
	if id != "" {
		q.ID = id
		set++
	}
	if vector != "" {
		if err := json.Unmarshal([]byte(vector), &q.Vector); err != nil {
			return q, fmt.Errorf("parse -vector as JSON array: %w", err)
		}
		set++
	}
	if text != "" {
		q.Text = text
		set++
	}
	if imagePath != "" {
		data, err := os.ReadFile(imagePath)
		if err != nil {
			return q, fmt.Errorf("read -image: %w", err)
		}
		q.Image = data
		set++
	}
	if set != 1 {
		return q, fmt.Errorf("provide exactly one query: -id, -vector, -text, or -image (got %d)", set)
	}
	return q, nil
}
