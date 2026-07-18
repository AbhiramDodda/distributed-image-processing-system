package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/vsearch"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestSimilar_disabledWhenUnconfigured(t *testing.T) {
	mux := http.NewServeMux()
	similarRoutes(mux, config.SearchConfig{}, nil, discardLog())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/similar", bytes.NewReader([]byte(`{}`)))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

func TestSimilar_vectorQuery(t *testing.T) {
	// Write a tiny local corpus so the handler loads a real index (no encoder needed
	// for a vector query).
	dir := t.TempDir()
	path := filepath.Join(dir, "embeddings.parquet")
	rows := []vsearch.Embedding{
		{ID: "east", Dataset: "d", Vector: []float32{1, 0}},
		{ID: "north", Dataset: "d", Vector: []float32{0, 1}},
	}
	var buf bytes.Buffer
	if err := vsearch.WriteEmbeddingsParquet(&buf, 2, rows); err != nil {
		t.Fatalf("write corpus: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	mux := http.NewServeMux()
	similarRoutes(mux, config.SearchConfig{IndexKey: path}, nil, discardLog())

	body, _ := json.Marshal(similarRequest{K: 1, Vector: []float32{1, 0}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/similar", bytes.NewReader(body))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Results []vsearch.Result `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Results) != 1 || got.Results[0].ID != "east" {
		t.Fatalf("results = %+v, want [east]", got.Results)
	}
}

func TestSimilar_textNeedsEncoder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "embeddings.parquet")
	var buf bytes.Buffer
	vsearch.WriteEmbeddingsParquet(&buf, 2, []vsearch.Embedding{{ID: "east", Vector: []float32{1, 0}}})
	os.WriteFile(path, buf.Bytes(), 0o644)

	mux := http.NewServeMux()
	similarRoutes(mux, config.SearchConfig{IndexKey: path}, nil, discardLog()) // no encoder_url

	body, _ := json.Marshal(similarRequest{K: 1, Text: "a dog"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/similar", bytes.NewReader(body))
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (text query with no encoder)", rec.Code)
	}
}
