package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/vsearch"
)

// similarRequest is the POST /v1/similar body. Exactly one query mode is set,
// mirroring the vsearch CLI: id/vector need no encoder; text/image_b64 are encoded
// live by the CLIP sidecar.
type similarRequest struct {
	K int `json:"k"`
	ID string `json:"id,omitempty"`
	Vector []float32 `json:"vector,omitempty"`
	Text string `json:"text,omitempty"`
	ImageB64 string `json:"image_b64,omitempty"`
}

// similarRoutes registers POST /v1/similar when vector search is configured. The
// index is loaded once, lazily, on first request (and cached) so an unqueried
// server pays nothing; text/image queries proxy to the configured CLIP sidecar.
func similarRoutes(mux *http.ServeMux, cfg config.SearchConfig, store *storage.Client, log *slog.Logger) {
	if cfg.IndexKey == "" {
		mux.HandleFunc("/v1/similar", func(w http.ResponseWriter, r *http.Request) {
			writeError(w, http.StatusNotImplemented, "vector search not configured (set server.search.index_key)")
		})
		return
	}

	var (
		once sync.Once
		ix *vsearch.Index
		loadErr error
	)
	load := func(ctx context.Context) (*vsearch.Index, error) {
		once.Do(func() {
			ix, loadErr = vsearch.LoadIndex(ctx, store, cfg.IndexKey)
			if loadErr == nil {
				log.Info("vector index loaded", "vectors", ix.Len(), "dim", ix.Dim(), "source", cfg.IndexKey)
			}
		})
		return ix, loadErr
	}

	var enc vsearch.Encoder
	if cfg.EncoderURL != "" {
		enc = vsearch.NewHTTPEncoder(cfg.EncoderURL, &http.Client{Timeout: 30 * time.Second})
	}

	mux.HandleFunc("/v1/similar", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		var req similarRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		index, err := load(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "load index: "+err.Error())
			return
		}

		q := vsearch.Query{K: req.K, ID: req.ID, Vector: req.Vector, Text: req.Text}
		if req.ImageB64 != "" {
			raw, err := base64.StdEncoding.DecodeString(req.ImageB64)
			if err != nil {
				writeError(w, http.StatusBadRequest, "image_b64: "+err.Error())
				return
			}
			q.Image = raw
		}

		results, err := index.Run(r.Context(), q, enc)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": results})
	})
}
