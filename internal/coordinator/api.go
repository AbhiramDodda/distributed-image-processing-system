package coordinator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

type API struct {
	coord *Coordinator
}

func NewAPI(coord *Coordinator) *API { return &API{coord: coord} }

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/v1/metrics/pending", a.handlePendingMetric)
	mux.HandleFunc("/v1/operator/drain", a.handleOperatorDrain)
	mux.HandleFunc("/v1/cluster/register", a.handleRegister)
	mux.HandleFunc("/v1/cluster/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("/v1/cluster/nodes", a.handleNodes)
	mux.HandleFunc("/v1/cluster/ring", a.handleRing)
	mux.HandleFunc("/v1/jobs", a.handleJobs)
	mux.HandleFunc("/v1/jobs/", a.handleJob)
	mux.HandleFunc("/v1/tasks/poll", a.handlePoll)
	mux.HandleFunc("/v1/tasks/", a.handleTask)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handlePendingMetric(w http.ResponseWriter, r *http.Request) {
	pending := a.coord.sched.PendingCount()
	active := len(a.coord.membership.ActiveNodes())
	var ratio float64
	if active > 0 {
		ratio = float64(pending) / float64(active)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pending_tasks":            pending,
		"active_workers":           active,
		"pending_tasks_per_worker": ratio,
	})
}

func (a *API) handleOperatorDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	n := 10
	if s := r.URL.Query().Get("n"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			n = v
		}
	}
	assignments := a.coord.sched.DrainPending(n)
	if assignments == nil {
		assignments = []scheduler.TaskAssignment{}
	}
	writeJSON(w, http.StatusOK, assignments)
}

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req cluster.RegisterRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.coord.membership.Register(req); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (a *API) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var hb cluster.Heartbeat
	if err := decode(r, &hb); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.coord.membership.Heartbeat(hb); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.coord.membership.AllNodes())
}

func (a *API) handleRing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_count":   a.coord.ring.NodeCount(),
		"distribution": a.coord.ring.Distribution(),
	})
}

func (a *API) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req scheduler.SubmitJobRequest
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		job, err := a.coord.sched.Submit(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, scheduler.SubmitJobResponse{
			JobID:      job.ID,
			TotalTasks: job.TotalTasks,
		})
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.coord.sched.ListJobs())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) handleJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	job, err := a.coord.sched.GetJob(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (a *API) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		writeError(w, http.StatusBadRequest, "worker query param required")
		return
	}
	assignment, err := a.coord.sched.PollTasks(workerID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, scheduler.PollResponse{
		Assignment: assignment,
		HasWork:    assignment != nil,
	})
}

func (a *API) handleTask(w http.ResponseWriter, r *http.Request) {
	// /v1/tasks/{id}/start  or  /v1/tasks/{id}/result
	path := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		writeError(w, http.StatusBadRequest, "invalid task path")
		return
	}
	taskID, action := parts[0], parts[1]

	switch action {
	case "start":
		var req scheduler.StartTaskRequest
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := a.coord.sched.StartTask(taskID, req.WorkerID); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
	case "result":
		var req scheduler.ResultRequest
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := a.coord.sched.ReportResult(r.Context(), taskID, req); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("unknown action: %s", action))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decode(r *http.Request, v any) error {
	if r.Body == nil {
		return fmt.Errorf("empty body")
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
