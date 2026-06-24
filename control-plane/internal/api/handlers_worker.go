package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

type registerWorkerRequest struct {
	Hostname      string                  `json:"hostname"`
	Address       string                  `json:"address"`
	Capacity      model.ResourceCapacity  `json:"capacity"`
	MaxConcurrent int                     `json:"max_concurrent_jobs"`
}

// RegisterWorker handles POST /api/v1/workers/register. A worker is
// considered healthy the instant it successfully registers, since calling
// this endpoint is itself proof of life.
func (h *Handlers) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Address == "" {
		writeJSONError(w, http.StatusBadRequest, "address is required")
		return
	}

	const maxIDAttempts = 5
	var id string
	for attempt := 0; attempt < maxIDAttempts; attempt++ {
		genID, err := generateID("worker")
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate worker id")
			return
		}
		now := time.Now()
		worker := &model.Worker{
			ID:              genID,
			Hostname:        req.Hostname,
			Address:         req.Address,
			Status:          model.WorkerRegistering,
			Capacity:        req.Capacity,
			Available:       req.Capacity,
			MaxConcurrent:   req.MaxConcurrent,
			LastHeartbeatAt: now,
			RegisteredAt:    now,
		}
		if err := h.store.RegisterWorker(r.Context(), worker); err == nil {
			id = genID
			break
		}
	}
	if id == "" {
		writeJSONError(w, http.StatusInternalServerError, "failed to allocate a unique worker id")
		return
	}

	if err := h.store.TransitionWorker(r.Context(), id, model.WorkerHealthy); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to mark worker healthy")
		return
	}

	worker, err := h.store.GetWorker(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "worker vanished after registration")
		return
	}
	writeJSON(w, http.StatusCreated, worker)
}

type heartbeatRequest struct {
	RunningJobs int                    `json:"running_jobs"`
	Available   model.ResourceCapacity `json:"available"`
}

// Heartbeat handles POST /api/v1/workers/{id}/heartbeat. Receiving a
// heartbeat always proves the worker is alive, so an UNHEALTHY worker
// recovers here -- the reconciler only ever owns the HEALTHY->UNHEALTHY
// direction. A DRAINING worker stays DRAINING (an operator decommission
// decision isn't undone by the worker simply still being alive).
func (h *Handlers) Heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	worker, err := h.store.GetWorker(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "worker not found")
		return
	}

	var req heartbeatRequest
	_ = json.NewDecoder(r.Body).Decode(&req) // body is optional

	worker.LastHeartbeatAt = time.Now()
	worker.RunningJobs = req.RunningJobs
	if req.Available != (model.ResourceCapacity{}) {
		worker.Available = req.Available
	}
	if err := h.store.UpdateWorker(r.Context(), worker); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if worker.Status == model.WorkerUnhealthy {
		if err := h.store.TransitionWorker(r.Context(), id, model.WorkerHealthy); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	metrics.WorkerHeartbeats.Inc()
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// PollJobs handles GET /api/v1/workers/{id}/jobs/poll. It returns at most
// one job currently SCHEDULED onto this worker (still awaiting pickup), or
// an empty object if there is none. This endpoint isn't in the spec's
// section-6 API list, which describes the worker's polling *behavior*
// without naming the endpoint -- it's a necessary, deliberate addition.
func (h *Handlers) PollJobs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetWorker(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "worker not found")
		return
	}

	jobs, err := h.store.ListJobsByWorker(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, j := range jobs {
		if j.Status == model.JobScheduled {
			writeJSON(w, http.StatusOK, map[string]any{"job": j})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": nil})
}

type jobStatusRequest struct {
	Status   model.JobStatus `json:"status"`
	ExitCode *int            `json:"exit_code,omitempty"`
	Error    string          `json:"error,omitempty"`
	Output   string          `json:"output,omitempty"`
}

// UpdateJobStatus handles POST /api/v1/workers/{id}/jobs/{job_id}/status.
// When a job leaves RUNNING into a terminal-ish state (SUCCEEDED, FAILED,
// CANCELLED), the worker's reserved capacity for it is released here --
// capacity is reserved at schedule time and freed at completion time, not
// adjusted on intermediate "now running" reports.
func (h *Handlers) UpdateJobStatus(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	jobID := r.PathValue("job_id")

	var req jobStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	job, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	wasRunning := job.Status == model.JobRunning

	job.ExitCode = req.ExitCode
	job.Output = req.Output
	if err := h.store.UpdateJob(r.Context(), job); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.store.TransitionJob(r.Context(), jobID, req.Status, req.Error); err != nil {
		if err == state.ErrInvalidTransition {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if wasRunning && isTerminalish(req.Status) {
		h.releaseWorkerCapacity(r.Context(), workerID, job.WorkloadID)
		switch req.Status {
		case model.JobSucceeded:
			metrics.JobsSucceeded.Inc()
		case model.JobFailed:
			metrics.JobsFailed.Inc()
		}
	}

	updated, _ := h.store.GetJob(r.Context(), jobID)
	writeJSON(w, http.StatusOK, updated)
}

func isTerminalish(s model.JobStatus) bool {
	switch s {
	case model.JobSucceeded, model.JobFailed, model.JobCancelled:
		return true
	default:
		return false
	}
}

func (h *Handlers) releaseWorkerCapacity(ctx context.Context, workerID, workloadID string) {
	worker, err := h.store.GetWorker(ctx, workerID)
	if err != nil {
		return
	}
	if worker.RunningJobs > 0 {
		worker.RunningJobs--
	}
	if workload, err := h.store.GetWorkload(ctx, workloadID); err == nil {
		worker.Available.CPU += workload.Resources.CPU
		worker.Available.MemoryMB += workload.Resources.MemoryMB
		if worker.Available.CPU > worker.Capacity.CPU {
			worker.Available.CPU = worker.Capacity.CPU
		}
		if worker.Available.MemoryMB > worker.Capacity.MemoryMB {
			worker.Available.MemoryMB = worker.Capacity.MemoryMB
		}
	}
	_ = h.store.UpdateWorker(ctx, worker)
}

// ListWorkers handles GET /api/v1/workers.
func (h *Handlers) ListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.store.ListWorkers(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers, "total": len(workers)})
}

// GetWorker handles GET /api/v1/workers/{id}.
func (h *Handlers) GetWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	worker, err := h.store.GetWorker(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "worker not found")
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

// DrainWorker handles POST /api/v1/workers/{id}/drain.
func (h *Handlers) DrainWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetWorker(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "worker not found")
		return
	}
	if err := h.store.TransitionWorker(r.Context(), id, model.WorkerDraining); err != nil {
		if err == state.ErrInvalidTransition {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	worker, _ := h.store.GetWorker(r.Context(), id)
	writeJSON(w, http.StatusOK, worker)
}
