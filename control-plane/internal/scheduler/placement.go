package scheduler

import (
	"errors"

	"github.com/czhao-dev/control-plane/internal/model"
)

// ErrNoCapacity is returned when no healthy worker has enough available
// capacity to take on a job.
var ErrNoCapacity = errors.New("no worker with sufficient capacity")

// SelectWorker picks the best healthy worker for a job requiring `required`
// resources among `candidates`, or returns ErrNoCapacity if none qualify.
//
// Policy: filter to HEALTHY workers with enough available capacity and
// concurrency headroom, then pick the least-loaded one (lowest RunningJobs),
// breaking ties by earliest RegisteredAt for determinism. Pending jobs are
// already processed in FIFO (CreatedAt) order by the caller, so this one
// function satisfies both the spec's "FIFO" and "resource-aware" scheduling
// requirements: FIFO ordering of which job is considered next, resource-aware
// placement for which worker it lands on.
func SelectWorker(candidates []*model.Worker, required model.ResourceRequest) (*model.Worker, error) {
	var best *model.Worker
	for _, w := range candidates {
		if w.Status != model.WorkerHealthy {
			continue
		}
		if !w.HasCapacityFor(required) {
			continue
		}
		if best == nil ||
			w.RunningJobs < best.RunningJobs ||
			(w.RunningJobs == best.RunningJobs && w.RegisteredAt.Before(best.RegisteredAt)) {
			best = w
		}
	}
	if best == nil {
		return nil, ErrNoCapacity
	}
	return best, nil
}
