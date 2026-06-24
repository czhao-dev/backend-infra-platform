package model

import "time"

// JobStatus is the lifecycle state of a single execution unit created from a Workload.
type JobStatus string

const (
	JobPending    JobStatus = "PENDING"
	JobScheduled  JobStatus = "SCHEDULED"
	JobRunning    JobStatus = "RUNNING"
	JobSucceeded  JobStatus = "SUCCEEDED"
	JobFailed     JobStatus = "FAILED"
	JobRetrying   JobStatus = "RETRYING"
	JobDeadLetter JobStatus = "DEAD_LETTER"
	JobCancelled  JobStatus = "CANCELLED"
)

// allowedJobTransitions maps a job status to the set of statuses it may move to.
// Terminal states (SUCCEEDED, DEAD_LETTER, CANCELLED) have no outgoing transitions.
var allowedJobTransitions = map[JobStatus][]JobStatus{
	JobPending: {JobScheduled, JobCancelled},
	// SCHEDULED -> PENDING covers the scheduler reverting a dispatch that the
	// worker never actually picked up (e.g. worker became unhealthy mid-dispatch).
	JobScheduled:  {JobRunning, JobPending, JobCancelled},
	JobRunning:    {JobSucceeded, JobFailed, JobCancelled},
	JobFailed:     {JobRetrying, JobDeadLetter},
	JobRetrying:   {JobPending, JobScheduled},
	JobSucceeded:  {},
	JobDeadLetter: {},
	JobCancelled:  {},
}

// TransitionJob reports whether moving a job from `from` to `to` is legal.
// It is pure and stateless; callers apply the transition atomically (see state.Store).
func TransitionJob(from, to JobStatus) bool {
	for _, s := range allowedJobTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Job is a concrete execution unit created from a Workload.
type Job struct {
	ID          string     `json:"id"`
	WorkloadID  string     `json:"workload_id"`
	WorkerID    string     `json:"worker_id,omitempty"`
	Attempt     int        `json:"attempt"`
	Status      JobStatus  `json:"status"`
	Command     string     `json:"command"`
	Args        []string   `json:"args"`
	ExitCode    *int       `json:"exit_code,omitempty"`
	Error       string     `json:"error,omitempty"`
	Output      string     `json:"output,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	RunAfter    time.Time  `json:"run_after,omitzero"`
}

// Clone returns a deep copy so callers can mutate it without racing on
// slice/pointer fields shared with a value stored elsewhere.
func (j Job) Clone() Job {
	c := j
	if j.Args != nil {
		c.Args = append([]string(nil), j.Args...)
	}
	if j.ExitCode != nil {
		v := *j.ExitCode
		c.ExitCode = &v
	}
	if j.ScheduledAt != nil {
		t := *j.ScheduledAt
		c.ScheduledAt = &t
	}
	if j.StartedAt != nil {
		t := *j.StartedAt
		c.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		c.FinishedAt = &t
	}
	return c
}

// Active reports whether the job still occupies one of a workload's desired
// replica slots. PENDING/SCHEDULED/RUNNING/RETRYING are still in flight;
// FAILED is a brief pre-retry-decision state the reconciler resolves on its
// next tick; SUCCEEDED permanently fills its slot (batch semantics — a
// workload's replicas represent "run this many job instances", not "keep N
// running forever"; perpetual restart-on-success for restart_policy=always
// services is a documented future extension, not implemented in v1).
// Only DEAD_LETTER and CANCELLED free a slot for the reconciler to refill.
func (j Job) Active() bool {
	switch j.Status {
	case JobPending, JobScheduled, JobRunning, JobRetrying, JobFailed, JobSucceeded:
		return true
	default:
		return false
	}
}
