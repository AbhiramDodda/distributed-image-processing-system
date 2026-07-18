package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/admission"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/diag"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

type API struct {
	coord *Coordinator
}

func NewAPI(coord *Coordinator) *API { return &API{coord: coord} }

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/v1/metrics/pending", a.handlePendingMetric)
	mux.HandleFunc("/v1/metrics/admission", a.handleAdmissionMetric)
	mux.HandleFunc("/v1/metrics/tasks", a.handleTaskMetric)
	mux.HandleFunc("/v1/operator/drain", a.handleOperatorDrain)
	mux.HandleFunc("/v1/cluster/register", a.handleRegister)
	mux.HandleFunc("/v1/cluster/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("/v1/cluster/nodes", a.handleNodes)
	mux.HandleFunc("/v1/cluster/ring", a.handleRing)
	mux.HandleFunc("/v1/jobs", a.handleJobs)
	mux.HandleFunc("/v1/jobs/", a.handleJob)
	mux.HandleFunc("/v1/tasks/poll", a.handlePoll)
	mux.HandleFunc("/v1/tasks/", a.handleTask)
	// Concurrency diagnostics: lock stats, invariant violations, lock-order
	// warnings, and (with ?stacks=1) a full goroutine dump. Always registered;
	// it reports enabled=false and is otherwise inert until PETABYTE_DIAG is set.
	mux.HandleFunc("/debug/diag", diag.Handler())
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
		"pending_tasks": pending,
		"active_workers": active,
		"pending_tasks_per_worker": ratio,
	})
}

// handleTaskMetric reports aggregate task state and completed-task latency
// (percentiles over StartedAt -> FinishedAt), plus the rebalance count that a
// failure/chaos run drives up. active_workers is included so latency can be read
// against the surviving capacity.
func (a *API) handleTaskMetric(w http.ResponseWriter, r *http.Request) {
	m := a.coord.sched.Metrics()
	writeJSON(w, http.StatusOK, map[string]any{
		"tasks_total": m.TasksTotal,
		"pending": m.Pending,
		"assigned": m.Assigned,
		"running": m.Running,
		"done": m.Done,
		"failed": m.Failed,
		"rebalances": m.Rebalances,
		"active_workers": len(a.coord.membership.ActiveNodes()),
		"latency_p50_ms": m.LatencyP50Ms,
		"latency_p95_ms": m.LatencyP95Ms,
		"latency_p99_ms": m.LatencyP99Ms,
		"latency_max_ms": m.LatencyMaxMs,
		"latency_mean_ms": m.LatencyMeanMs,
	})
}

func (a *API) handleAdmissionMetric(w http.ResponseWriter, r *http.Request) {
	if a.coord.admission == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	s := a.coord.admission.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": true,
		"max_in_flight": s.MaxInFlight,
		"in_flight": s.InFlight,
		"admitted_total": s.Admitted,
		"rejected_total": s.Rejected,
		"per_tenant_in_flight": s.PerTenant,
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
		"node_count": a.coord.ring.NodeCount(),
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
		// Backpressure: admit (or shed) the job before it is scheduled. A rejection
		// is a 429 with Retry-After, not an error -- the platform is protecting its
		// stable operating point, and the client should back off and retry.
		var ticket *admission.Ticket
		if a.coord.admission != nil {
			tenant := r.Header.Get("X-Tenant")
			if tenant == "" {
				tenant = "default"
			}
			tk, err := a.coord.admission.Admit(tenant)
			if err != nil {
				if errors.Is(err, admission.ErrRejected) {
					w.Header().Set("Retry-After", "1")
					writeError(w, http.StatusTooManyRequests, err.Error())
					return
				}
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			ticket = tk
		}
		job, err := a.coord.sched.Submit(req)
		if err != nil {
			if ticket != nil {
				ticket.Release()
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if ticket != nil {
			a.coord.trackTicket(job.ID, ticket)
		}
		writeJSON(w, http.StatusCreated, scheduler.SubmitJobResponse{
			JobID: job.ID,
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
		HasWork: assignment != nil,
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
	case "renew":
		var req scheduler.RenewLeaseRequest
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		renewal, err := a.coord.sched.RenewLease(taskID, req)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, renewal)
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
