package model

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransitionJob(t *testing.T) {
	allStatuses := []JobStatus{
		JobPending, JobScheduled, JobRunning, JobSucceeded,
		JobFailed, JobRetrying, JobDeadLetter, JobCancelled,
	}

	valid := map[JobStatus]map[JobStatus]bool{
		JobPending:    {JobScheduled: true, JobCancelled: true},
		JobScheduled:  {JobRunning: true, JobPending: true, JobCancelled: true},
		JobRunning:    {JobSucceeded: true, JobFailed: true, JobCancelled: true},
		JobFailed:     {JobRetrying: true, JobDeadLetter: true},
		JobRetrying:   {JobPending: true, JobScheduled: true},
		JobSucceeded:  {},
		JobDeadLetter: {},
		JobCancelled:  {},
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			from, to := from, to
			want := valid[from][to]
			t.Run(fmt.Sprintf("%s_to_%s", from, to), func(t *testing.T) {
				assert.Equal(t, want, TransitionJob(from, to))
			})
		}
	}
}

func TestJobClone(t *testing.T) {
	exitCode := 0
	job := Job{ID: "job_1", Args: []string{"a", "b"}, ExitCode: &exitCode}
	clone := job.Clone()

	clone.Args[0] = "mutated"
	*clone.ExitCode = 99

	assert.Equal(t, "a", job.Args[0], "mutating a clone must not affect the original")
	assert.Equal(t, 0, *job.ExitCode, "mutating a clone's ExitCode must not affect the original")
}

func TestJobActive(t *testing.T) {
	cases := []struct {
		status JobStatus
		active bool
	}{
		{JobPending, true},
		{JobScheduled, true},
		{JobRunning, true},
		{JobRetrying, true},
		{JobFailed, true},
		{JobSucceeded, true},
		{JobDeadLetter, false},
		{JobCancelled, false},
	}
	for _, c := range cases {
		job := Job{Status: c.status}
		assert.Equal(t, c.active, job.Active(), "status %s", c.status)
	}
}
