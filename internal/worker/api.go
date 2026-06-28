package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type API struct {
	w *Worker
}

func NewAPI(w *Worker) *API { return &API{w: w} }

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", a.handleHealth)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":       "ok",
		"worker_id":    a.w.ID(),
		"active_tasks": a.w.ActiveTaskCount(),
	})
}

func postJSON(url string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e map[string]string
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server %d: %s", resp.StatusCode, e["error"])
	}
	return nil
}
