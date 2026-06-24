// Package agentmetrics defines Prometheus metrics for the worker AGENT
// binary (cmd/worker) only. Kept separate from internal/metrics (the
// control-plane binary's metrics) so each binary's process-global
// Prometheus registry only contains the metrics it actually owns --
// importing both metric sets into one shared package would mean every
// worker's /metrics endpoint also exposes (always-zero) control-plane
// metrics, and vice versa, since promauto registers into the importing
// process's default registry regardless of which functions are ever called.
package agentmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WorkerRunningJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "worker_running_jobs",
		Help: "Jobs currently executing on this worker",
	})

	WorkerCompletedJobsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "worker_completed_jobs_total",
		Help: "Total jobs completed successfully by this worker",
	})

	WorkerFailedJobsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "worker_failed_jobs_total",
		Help: "Total jobs that failed on this worker",
	})

	WorkerHeartbeatLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_heartbeat_latency_seconds",
		Help:    "Round-trip latency of heartbeat requests to the control plane",
		Buckets: prometheus.DefBuckets,
	})
)
