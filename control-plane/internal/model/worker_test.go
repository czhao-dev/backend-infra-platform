package model

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransitionWorker(t *testing.T) {
	allStatuses := []WorkerStatus{
		WorkerRegistering, WorkerHealthy, WorkerDraining, WorkerUnhealthy, WorkerRemoved,
	}

	valid := map[WorkerStatus]map[WorkerStatus]bool{
		WorkerRegistering: {WorkerHealthy: true, WorkerUnhealthy: true},
		WorkerHealthy:     {WorkerDraining: true, WorkerUnhealthy: true},
		WorkerDraining:    {WorkerRemoved: true, WorkerUnhealthy: true},
		WorkerUnhealthy:   {WorkerHealthy: true, WorkerRemoved: true},
		WorkerRemoved:     {},
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			from, to := from, to
			want := valid[from][to]
			t.Run(fmt.Sprintf("%s_to_%s", from, to), func(t *testing.T) {
				assert.Equal(t, want, TransitionWorker(from, to))
			})
		}
	}
}

func TestWorkerHasCapacityFor(t *testing.T) {
	w := Worker{
		Available:     ResourceCapacity{CPU: 1, MemoryMB: 512},
		RunningJobs:   2,
		MaxConcurrent: 2,
	}
	assert.False(t, w.HasCapacityFor(ResourceRequest{CPU: 0.1, MemoryMB: 1}), "at max concurrency, no capacity regardless of resources")

	w.RunningJobs = 1
	assert.True(t, w.HasCapacityFor(ResourceRequest{CPU: 1, MemoryMB: 512}))
	assert.False(t, w.HasCapacityFor(ResourceRequest{CPU: 1.1, MemoryMB: 1}))
	assert.False(t, w.HasCapacityFor(ResourceRequest{CPU: 0.1, MemoryMB: 513}))

	w.MaxConcurrent = 0 // unlimited concurrency
	assert.True(t, w.HasCapacityFor(ResourceRequest{CPU: 1, MemoryMB: 512}))
}
