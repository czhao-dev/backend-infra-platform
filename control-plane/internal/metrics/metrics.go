// Package metrics defines Prometheus metrics for the control-plane binary
// (cmd/control-plane) only -- see internal/agentmetrics for the worker
// agent's metrics, kept in a separate package so each binary's process-
// global registry only contains the metrics it actually owns. All metric
// names are prefixed ctrlplane_ to avoid collisions with
// ml-job-orchestrator's mlorch_* and reverse-proxy-load-balancer's proxy_*
// metrics scraped by the same Prometheus instance.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Control plane metrics.
var (
	WorkloadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_workloads_total",
		Help: "Total workloads submitted",
	})

	JobsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_jobs_total",
		Help: "Total jobs created",
	})

	// JobsPending/JobsRunning are gauges (current counts), not counters --
	// the spec names them as "_total" but a count of currently-pending jobs
	// cannot be monotonic.
	JobsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ctrlplane_jobs_pending",
		Help: "Jobs currently pending",
	})

	JobsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ctrlplane_jobs_running",
		Help: "Jobs currently running",
	})

	JobsSucceeded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_jobs_succeeded_total",
		Help: "Total jobs that succeeded",
	})

	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_jobs_failed_total",
		Help: "Total job attempts that failed (including ones later retried)",
	})

	JobsDeadLetter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_jobs_dead_letter_total",
		Help: "Total jobs that exhausted their retry budget and were dead-lettered",
	})

	SchedulerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ctrlplane_scheduler_queue_depth",
		Help: "Number of jobs currently pending scheduling",
	})

	SchedulerLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ctrlplane_scheduler_latency_seconds",
		Help:    "Time from job creation to being scheduled onto a worker",
		Buckets: prometheus.DefBuckets,
	})

	ReconcilerIterations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_reconciler_iterations_total",
		Help: "Total reconciler loop iterations",
	})

	WorkerHeartbeats = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ctrlplane_worker_heartbeats_total",
		Help: "Total worker heartbeats received",
	})

	UnhealthyWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ctrlplane_unhealthy_workers",
		Help: "Number of workers currently marked unhealthy",
	})
)
