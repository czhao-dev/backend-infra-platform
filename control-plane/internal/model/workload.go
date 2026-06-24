package model

import "time"

// WorkloadType distinguishes a one-shot batch of job instances from a
// long-running service-style workload.
type WorkloadType string

const (
	WorkloadBatch   WorkloadType = "batch"
	WorkloadService WorkloadType = "service"
)

// RestartPolicy controls whether a finished job's replica slot is refilled.
type RestartPolicy string

const (
	RestartNever     RestartPolicy = "never"
	RestartOnFailure RestartPolicy = "on_failure"
	RestartAlways    RestartPolicy = "always"
)

// WorkloadStatus is the lifecycle state of desired state submitted by a user.
type WorkloadStatus string

const (
	WorkloadPending   WorkloadStatus = "PENDING"
	WorkloadActive    WorkloadStatus = "ACTIVE"
	WorkloadDegraded  WorkloadStatus = "DEGRADED" // some replicas dead-lettered/unhealthy
	WorkloadCancelled WorkloadStatus = "CANCELLED"
)

var allowedWorkloadTransitions = map[WorkloadStatus][]WorkloadStatus{
	WorkloadPending:   {WorkloadActive, WorkloadCancelled},
	WorkloadActive:    {WorkloadDegraded, WorkloadCancelled},
	WorkloadDegraded:  {WorkloadActive, WorkloadCancelled},
	WorkloadCancelled: {},
}

// TransitionWorkload reports whether moving a workload from `from` to `to` is legal.
func TransitionWorkload(from, to WorkloadStatus) bool {
	for _, s := range allowedWorkloadTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// ResourceRequest is the resources a single job instance of a workload requires.
type ResourceRequest struct {
	CPU      float64 `json:"cpu"`
	MemoryMB int     `json:"memory_mb"`
}

// Workload represents desired state submitted by a user.
type Workload struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Type          WorkloadType    `json:"type"`
	Command       string          `json:"command"`
	Args          []string        `json:"args"`
	Replicas      int             `json:"replicas"`
	MaxRetries    int             `json:"max_retries"`
	RestartPolicy RestartPolicy   `json:"restart_policy"`
	Resources     ResourceRequest `json:"resources"`
	Status        WorkloadStatus  `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// Clone returns a deep copy so callers can mutate it without racing on
// slice fields shared with a value stored elsewhere.
func (w Workload) Clone() Workload {
	c := w
	if w.Args != nil {
		c.Args = append([]string(nil), w.Args...)
	}
	return c
}
