package model

import "time"

// WorkerStatus is the lifecycle state of a registered execution node.
type WorkerStatus string

const (
	WorkerRegistering WorkerStatus = "REGISTERING"
	WorkerHealthy     WorkerStatus = "HEALTHY"
	WorkerDraining    WorkerStatus = "DRAINING"
	WorkerUnhealthy   WorkerStatus = "UNHEALTHY"
	WorkerRemoved     WorkerStatus = "REMOVED"
)

var allowedWorkerTransitions = map[WorkerStatus][]WorkerStatus{
	WorkerRegistering: {WorkerHealthy, WorkerUnhealthy},
	WorkerHealthy:     {WorkerDraining, WorkerUnhealthy},
	WorkerDraining:    {WorkerRemoved, WorkerUnhealthy},
	// A worker recovers (resumes heartbeating) before the reconciler removes it.
	WorkerUnhealthy: {WorkerHealthy, WorkerRemoved},
	WorkerRemoved:   {},
}

// TransitionWorker reports whether moving a worker from `from` to `to` is legal.
func TransitionWorker(from, to WorkerStatus) bool {
	for _, s := range allowedWorkerTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// ResourceCapacity describes a worker's total or currently-available resources.
type ResourceCapacity struct {
	CPU      float64 `json:"cpu"`
	MemoryMB int     `json:"memory_mb"`
}

// Worker represents a registered execution node (a running worker agent process).
type Worker struct {
	ID              string           `json:"id"`
	Hostname        string           `json:"hostname"`
	Address         string           `json:"address"`
	Status          WorkerStatus     `json:"status"`
	Capacity        ResourceCapacity `json:"capacity"`
	Available       ResourceCapacity `json:"available"`
	RunningJobs     int              `json:"running_jobs"`
	MaxConcurrent   int              `json:"max_concurrent_jobs"`
	LastHeartbeatAt time.Time        `json:"last_heartbeat_at"`
	RegisteredAt    time.Time        `json:"registered_at"`
}

// Clone returns a deep copy of the Worker.
func (w Worker) Clone() Worker {
	return w
}

// HasCapacityFor reports whether the worker currently has enough available
// resources and concurrency headroom to take on a job requiring `req`.
func (w Worker) HasCapacityFor(req ResourceRequest) bool {
	if w.MaxConcurrent > 0 && w.RunningJobs >= w.MaxConcurrent {
		return false
	}
	return w.Available.CPU >= req.CPU && w.Available.MemoryMB >= req.MemoryMB
}
