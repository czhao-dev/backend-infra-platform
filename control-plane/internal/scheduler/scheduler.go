package scheduler

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

// Scheduler assigns PENDING jobs to HEALTHY workers with available capacity
// on a fixed poll interval -- mirrors ml-job-orchestrator's poll-based
// scheduler loop shape.
type Scheduler struct {
	store    state.Store
	interval time.Duration
	logger   *slog.Logger

	mu    sync.Mutex
	stats Stats
}

// Stats is a snapshot of the scheduler's most recent tick, exposed via
// GET /api/v1/scheduler/stats.
type Stats struct {
	LastTickAt    time.Time `json:"last_tick_at"`
	ScheduledLast int       `json:"scheduled_last_tick"`
	PendingNow    int       `json:"pending_now"`
}

func New(st state.Store, interval time.Duration, logger *slog.Logger) *Scheduler {
	return &Scheduler{store: st, interval: interval, logger: logger}
}

// Run blocks, ticking every interval until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Tick(ctx)
		}
	}
}

// Tick runs one scheduling pass. Exported so the "rebalance" API and tests
// can trigger it synchronously outside the regular ticker cadence.
func (s *Scheduler) Tick(ctx context.Context) {
	pending, err := s.store.ListJobsByStatus(ctx, model.JobPending)
	if err != nil {
		s.logger.Error("scheduler: list pending jobs", "error", err)
		return
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].CreatedAt.Before(pending[j].CreatedAt) })

	workers, err := s.store.ListWorkers(ctx)
	if err != nil {
		s.logger.Error("scheduler: list workers", "error", err)
		return
	}

	now := time.Now()
	scheduled := 0
	for _, job := range pending {
		if !job.RunAfter.IsZero() && job.RunAfter.After(now) {
			continue // backoff gate set by the reconciler; not ready yet
		}

		var required model.ResourceRequest
		if workload, err := s.store.GetWorkload(ctx, job.WorkloadID); err == nil {
			required = workload.Resources
		}

		worker, err := SelectWorker(workers, required)
		if err != nil {
			continue // no capacity right now; retry next tick
		}

		// Set WorkerID via UpdateJob first (job.Status still matches the
		// stored PENDING value here), then flip status via TransitionJob --
		// doing it in the other order would have TransitionJob's internal
		// load/modify/store clobber the WorkerID set on this local copy.
		job.WorkerID = worker.ID
		if err := s.store.UpdateJob(ctx, job); err != nil {
			s.logger.Warn("scheduler: assign worker", "job_id", job.ID, "error", err)
			continue
		}
		if err := s.store.TransitionJob(ctx, job.ID, model.JobScheduled, ""); err != nil {
			s.logger.Warn("scheduler: transition job", "job_id", job.ID, "error", err)
			continue
		}

		worker.Available.CPU -= required.CPU
		worker.Available.MemoryMB -= required.MemoryMB
		worker.RunningJobs++
		if err := s.store.UpdateWorker(ctx, worker); err != nil {
			s.logger.Warn("scheduler: update worker capacity", "worker_id", worker.ID, "error", err)
		}

		metrics.SchedulerLatencySeconds.Observe(time.Since(job.CreatedAt).Seconds())
		scheduled++
	}

	metrics.SchedulerQueueDepth.Set(float64(len(pending) - scheduled))

	s.mu.Lock()
	s.stats = Stats{LastTickAt: time.Now(), ScheduledLast: scheduled, PendingNow: len(pending) - scheduled}
	s.mu.Unlock()
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
