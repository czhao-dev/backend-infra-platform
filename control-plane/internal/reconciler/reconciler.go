package reconciler

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

// Reconciler maintains desired state under failures: it creates/cancels
// jobs to match each workload's desired replica count, and detects worker
// heartbeat timeouts, marking workers unhealthy and rescheduling their
// in-flight jobs.
type Reconciler struct {
	store            state.Store
	interval         time.Duration
	heartbeatTimeout time.Duration
	logger           *slog.Logger
}

func New(st state.Store, interval, heartbeatTimeout time.Duration, logger *slog.Logger) *Reconciler {
	return &Reconciler{store: st, interval: interval, heartbeatTimeout: heartbeatTimeout, logger: logger}
}

// Run blocks, ticking every interval until ctx is cancelled.
func (rc *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(rc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rc.Tick(ctx)
		}
	}
}

// Tick runs one reconciliation pass (replica reconciliation, then heartbeat
// timeout detection). Exported so tests and the "rebalance"-adjacent demo
// scripts can trigger it synchronously.
func (rc *Reconciler) Tick(ctx context.Context) {
	rc.reconcileWorkloads(ctx)
	rc.detectUnhealthyWorkers(ctx)
	metrics.ReconcilerIterations.Inc()
}

func (rc *Reconciler) reconcileWorkloads(ctx context.Context) {
	workloads, err := rc.store.ListWorkloads(ctx)
	if err != nil {
		rc.logger.Error("reconciler: list workloads", "error", err)
		return
	}

	for _, wl := range workloads {
		if wl.Status == model.WorkloadCancelled {
			continue
		}
		if wl.Status == model.WorkloadPending {
			if err := rc.store.TransitionWorkload(ctx, wl.ID, model.WorkloadActive); err != nil {
				rc.logger.Warn("reconciler: activate workload", "workload_id", wl.ID, "error", err)
				continue
			}
			wl.Status = model.WorkloadActive
		}

		jobs, err := rc.store.ListJobsByWorkload(ctx, wl.ID)
		if err != nil {
			rc.logger.Error("reconciler: list jobs", "workload_id", wl.ID, "error", err)
			continue
		}

		active := 0
		hasDeadLetter := false
		var cancellable []*model.Job // PENDING/SCHEDULED, candidates for scale-down
		for _, j := range jobs {
			if j.Active() {
				active++
			}
			if j.Status == model.JobDeadLetter {
				hasDeadLetter = true
			}
			if j.Status == model.JobPending || j.Status == model.JobScheduled {
				cancellable = append(cancellable, j)
			}
		}

		switch {
		case active < wl.Replicas:
			for i := 0; i < wl.Replicas-active; i++ {
				id, err := newJobID()
				if err != nil {
					rc.logger.Error("reconciler: generate job id", "error", err)
					continue
				}
				job := &model.Job{
					ID:         id,
					WorkloadID: wl.ID,
					Status:     model.JobPending,
					Command:    wl.Command,
					Args:       wl.Args,
					CreatedAt:  time.Now(),
				}
				if err := rc.store.CreateJob(ctx, job); err != nil {
					rc.logger.Error("reconciler: create job", "workload_id", wl.ID, "error", err)
					continue
				}
				metrics.JobsTotal.Inc()
			}
		case active > wl.Replicas:
			excess := active - wl.Replicas
			sort.Slice(cancellable, func(i, j int) bool {
				return cancellable[i].CreatedAt.After(cancellable[j].CreatedAt) // newest first
			})
			for i := 0; i < excess && i < len(cancellable); i++ {
				if err := rc.store.TransitionJob(ctx, cancellable[i].ID, model.JobCancelled, "workload scaled down"); err != nil {
					rc.logger.Warn("reconciler: cancel excess job", "job_id", cancellable[i].ID, "error", err)
				}
			}
		}

		switch {
		case hasDeadLetter && wl.Status == model.WorkloadActive:
			_ = rc.store.TransitionWorkload(ctx, wl.ID, model.WorkloadDegraded)
		case !hasDeadLetter && wl.Status == model.WorkloadDegraded:
			_ = rc.store.TransitionWorkload(ctx, wl.ID, model.WorkloadActive)
		}
	}
}

func (rc *Reconciler) detectUnhealthyWorkers(ctx context.Context) {
	workers, err := rc.store.ListWorkers(ctx)
	if err != nil {
		rc.logger.Error("reconciler: list workers", "error", err)
		return
	}

	now := time.Now()
	unhealthyCount := 0
	for _, wk := range workers {
		if wk.Status == model.WorkerUnhealthy {
			unhealthyCount++
		}
		timedOut := now.Sub(wk.LastHeartbeatAt) > rc.heartbeatTimeout

		switch {
		case wk.Status == model.WorkerHealthy && timedOut:
			if err := rc.store.TransitionWorker(ctx, wk.ID, model.WorkerUnhealthy); err != nil {
				rc.logger.Warn("reconciler: mark worker unhealthy", "worker_id", wk.ID, "error", err)
				continue
			}
			unhealthyCount++
			rc.logger.Warn("reconciler: worker heartbeat timeout", "worker_id", wk.ID)
			rc.rescheduleJobsFor(ctx, wk.ID)

		case wk.Status == model.WorkerDraining && timedOut:
			// An operator-initiated decommission that's gone quiet: remove
			// rather than mark unhealthy.
			if err := rc.store.TransitionWorker(ctx, wk.ID, model.WorkerRemoved); err != nil {
				rc.logger.Warn("reconciler: remove drained worker", "worker_id", wk.ID, "error", err)
				continue
			}
			rc.rescheduleJobsFor(ctx, wk.ID)
		}
	}
	metrics.UnhealthyWorkers.Set(float64(unhealthyCount))
}

// rescheduleJobsFor requeues (with backoff) or dead-letters every RUNNING
// job assigned to a worker that just went unhealthy/removed. We can't know
// whether the job actually died or the worker just had a network blip, so
// we pessimistically requeue it.
func (rc *Reconciler) rescheduleJobsFor(ctx context.Context, workerID string) {
	jobs, err := rc.store.ListJobsByWorker(ctx, workerID)
	if err != nil {
		rc.logger.Error("reconciler: list jobs by worker", "worker_id", workerID, "error", err)
		return
	}

	for _, j := range jobs {
		// A job that was SCHEDULED but never reached RUNNING (the worker
		// died between dispatch and pickup) never actually executed, so it
		// doesn't burn a retry attempt -- just send it straight back to
		// PENDING (a legal SCHEDULED->PENDING transition) for the scheduler
		// to reassign.
		if j.Status == model.JobScheduled {
			j.WorkerID = ""
			if err := rc.store.UpdateJob(ctx, j); err != nil {
				rc.logger.Warn("reconciler: clear worker on orphaned scheduled job", "job_id", j.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionJob(ctx, j.ID, model.JobPending, ""); err != nil {
				rc.logger.Warn("reconciler: requeue orphaned scheduled job", "job_id", j.ID, "error", err)
			}
			continue
		}
		if j.Status != model.JobRunning {
			continue
		}

		workload, err := rc.store.GetWorkload(ctx, j.WorkloadID)
		maxRetries := 0
		if err == nil {
			maxRetries = workload.MaxRetries
		}

		newAttempt := j.Attempt + 1
		if newAttempt <= maxRetries {
			j.Attempt = newAttempt
			j.RunAfter = time.Now().Add(backoff(newAttempt))
			j.WorkerID = ""
			if err := rc.store.UpdateJob(ctx, j); err != nil {
				rc.logger.Warn("reconciler: bump job attempt", "job_id", j.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionJob(ctx, j.ID, model.JobFailed, "worker heartbeat timeout"); err != nil {
				rc.logger.Warn("reconciler: transition job to failed", "job_id", j.ID, "error", err)
				continue
			}
			_ = rc.store.TransitionJob(ctx, j.ID, model.JobRetrying, "")
			_ = rc.store.TransitionJob(ctx, j.ID, model.JobPending, "")
			metrics.JobsFailed.Inc()
		} else {
			j.Attempt = newAttempt
			if err := rc.store.UpdateJob(ctx, j); err != nil {
				rc.logger.Warn("reconciler: bump job attempt", "job_id", j.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionJob(ctx, j.ID, model.JobFailed, "worker heartbeat timeout"); err != nil {
				rc.logger.Warn("reconciler: transition job to failed", "job_id", j.ID, "error", err)
				continue
			}
			_ = rc.store.TransitionJob(ctx, j.ID, model.JobDeadLetter, "max retries exceeded after worker failure")
			metrics.JobsFailed.Inc()
			metrics.JobsDeadLetter.Inc()
		}
	}
}
