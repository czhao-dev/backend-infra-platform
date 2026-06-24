package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter wires every control-plane endpoint. Uses stdlib net/http's
// Go-1.22+ method-prefixed patterns and {wildcard} path values rather than
// gorilla/mux -- this is a fresh module on Go 1.26, so stdlib routing covers
// everything gorilla/mux would, consistent with how
// reverse-proxy-load-balancer already uses a plain ServeMux.
func NewRouter(h *Handlers) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/workloads", h.CreateWorkload)
	mux.HandleFunc("GET /api/v1/workloads", h.ListWorkloads)
	mux.HandleFunc("GET /api/v1/workloads/{id}", h.GetWorkload)
	mux.HandleFunc("DELETE /api/v1/workloads/{id}", h.CancelWorkload)
	mux.HandleFunc("GET /api/v1/workloads/{id}/jobs", h.ListWorkloadJobs)

	mux.HandleFunc("POST /api/v1/workers/register", h.RegisterWorker)
	mux.HandleFunc("POST /api/v1/workers/{id}/heartbeat", h.Heartbeat)
	mux.HandleFunc("GET /api/v1/workers/{id}/jobs/poll", h.PollJobs)
	mux.HandleFunc("POST /api/v1/workers/{id}/jobs/{job_id}/status", h.UpdateJobStatus)
	mux.HandleFunc("GET /api/v1/workers", h.ListWorkers)
	mux.HandleFunc("GET /api/v1/workers/{id}", h.GetWorker)
	mux.HandleFunc("POST /api/v1/workers/{id}/drain", h.DrainWorker)

	mux.HandleFunc("GET /api/v1/scheduler/queue", h.SchedulerQueue)
	mux.HandleFunc("GET /api/v1/scheduler/stats", h.SchedulerStats)
	mux.HandleFunc("POST /api/v1/scheduler/rebalance", h.SchedulerRebalance)

	mux.HandleFunc("GET /api/v1/routes", h.ListRoutes)
	mux.HandleFunc("POST /api/v1/routes", h.CreateRoute)
	mux.HandleFunc("GET /api/v1/routes/{id}", h.GetRoute)
	mux.HandleFunc("PUT /api/v1/routes/{id}", h.UpdateRoute)
	mux.HandleFunc("DELETE /api/v1/routes/{id}", h.DeleteRoute)

	mux.HandleFunc("GET /api/v1/proxy/config", h.ProxyConfig)
	mux.HandleFunc("GET /api/v1/proxy/backends", h.ProxyBackends)

	mux.HandleFunc("GET /healthz", Healthz)
	mux.HandleFunc("GET /readyz", Readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	return requestIDMiddleware(recoveryMiddleware(loggingMiddleware(mux)))
}

// Healthz handles GET /healthz -- liveness: the HTTP server is up.
func Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz handles GET /readyz -- readiness: always true once the server is
// serving, since the in-memory store has no external dependencies to wait on.
func Readyz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
