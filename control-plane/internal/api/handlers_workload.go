package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

type submitWorkloadRequest struct {
	Name          string                 `json:"name"`
	Type          model.WorkloadType     `json:"type"`
	Command       string                 `json:"command"`
	Args          []string               `json:"args"`
	Replicas      int                    `json:"replicas"`
	MaxRetries    int                    `json:"max_retries"`
	RestartPolicy model.RestartPolicy    `json:"restart_policy"`
	Resources     model.ResourceRequest  `json:"resources"`
}

// CreateWorkload handles POST /api/v1/workloads.
func (h *Handlers) CreateWorkload(w http.ResponseWriter, r *http.Request) {
	var req submitWorkloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" || req.Command == "" {
		writeJSONError(w, http.StatusBadRequest, "name and command are required")
		return
	}
	if req.Replicas <= 0 {
		req.Replicas = 1
	}
	if req.Type == "" {
		req.Type = model.WorkloadBatch
	}
	if req.RestartPolicy == "" {
		req.RestartPolicy = model.RestartOnFailure
	}

	const maxIDAttempts = 5
	var id string
	for attempt := 0; attempt < maxIDAttempts; attempt++ {
		genID, err := generateID("workload")
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate workload id")
			return
		}
		now := time.Now()
		workload := &model.Workload{
			ID:            genID,
			Name:          req.Name,
			Type:          req.Type,
			Command:       req.Command,
			Args:          req.Args,
			Replicas:      req.Replicas,
			MaxRetries:    req.MaxRetries,
			RestartPolicy: req.RestartPolicy,
			Resources:     req.Resources,
			Status:        model.WorkloadPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := h.store.CreateWorkload(r.Context(), workload); err == nil {
			id = genID
			break
		}
	}
	if id == "" {
		writeJSONError(w, http.StatusInternalServerError, "failed to allocate a unique workload id")
		return
	}

	metrics.WorkloadsTotal.Inc()

	workload, err := h.store.GetWorkload(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "workload vanished after creation")
		return
	}
	w.Header().Set("Location", "/api/v1/workloads/"+id)
	writeJSON(w, http.StatusCreated, workload)
}

// ListWorkloads handles GET /api/v1/workloads.
func (h *Handlers) ListWorkloads(w http.ResponseWriter, r *http.Request) {
	workloads, err := h.store.ListWorkloads(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workloads": workloads, "total": len(workloads)})
}

// GetWorkload handles GET /api/v1/workloads/{id}.
func (h *Handlers) GetWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	workload, err := h.store.GetWorkload(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "workload not found")
		return
	}
	writeJSON(w, http.StatusOK, workload)
}

// CancelWorkload handles DELETE /api/v1/workloads/{id}. It marks the
// workload CANCELLED; the reconciler stops refilling its replica slots on
// its next pass (running jobs are left to finish, see reconciler scale-down
// semantics).
func (h *Handlers) CancelWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	workload, err := h.store.GetWorkload(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "workload not found")
		return
	}
	if err := h.store.TransitionWorkload(r.Context(), id, model.WorkloadCancelled); err != nil {
		if err == state.ErrInvalidTransition {
			writeJSONError(w, http.StatusConflict, "workload already cancelled")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	workload, _ = h.store.GetWorkload(r.Context(), id)
	writeJSON(w, http.StatusOK, workload)
}

// ListWorkloadJobs handles GET /api/v1/workloads/{id}/jobs.
func (h *Handlers) ListWorkloadJobs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetWorkload(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "workload not found")
		return
	}
	jobs, err := h.store.ListJobsByWorkload(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": len(jobs)})
}
