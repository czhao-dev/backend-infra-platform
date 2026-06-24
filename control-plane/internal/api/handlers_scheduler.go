package api

import (
	"net/http"

	"github.com/czhao-dev/control-plane/internal/model"
)

// SchedulerQueue handles GET /api/v1/scheduler/queue.
func (h *Handlers) SchedulerQueue(w http.ResponseWriter, r *http.Request) {
	pending, err := h.store.ListJobsByStatus(r.Context(), model.JobPending)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": pending, "depth": len(pending)})
}

// SchedulerStats handles GET /api/v1/scheduler/stats.
func (h *Handlers) SchedulerStats(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "scheduler not running")
		return
	}
	writeJSON(w, http.StatusOK, h.scheduler.Stats())
}

// SchedulerRebalance handles POST /api/v1/scheduler/rebalance. It triggers
// an immediate scheduling pass synchronously rather than waiting for the
// next tick of the scheduler's regular poll interval.
func (h *Handlers) SchedulerRebalance(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "scheduler not running")
		return
	}
	h.scheduler.Tick(r.Context())
	writeJSON(w, http.StatusAccepted, h.scheduler.Stats())
}
