package scheduler

import (
	"testing"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectWorker_NoCandidates(t *testing.T) {
	_, err := SelectWorker(nil, model.ResourceRequest{})
	assert.ErrorIs(t, err, ErrNoCapacity)
}

func TestSelectWorker_AllUnhealthy(t *testing.T) {
	candidates := []*model.Worker{
		{ID: "w1", Status: model.WorkerUnhealthy, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
		{ID: "w2", Status: model.WorkerDraining, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
	}
	_, err := SelectWorker(candidates, model.ResourceRequest{CPU: 1, MemoryMB: 1})
	assert.ErrorIs(t, err, ErrNoCapacity)
}

func TestSelectWorker_AllOverCapacity(t *testing.T) {
	candidates := []*model.Worker{
		{ID: "w1", Status: model.WorkerHealthy, Available: model.ResourceCapacity{CPU: 0.1, MemoryMB: 10}},
	}
	_, err := SelectWorker(candidates, model.ResourceRequest{CPU: 1, MemoryMB: 100})
	assert.ErrorIs(t, err, ErrNoCapacity)
}

func TestSelectWorker_ExactFit(t *testing.T) {
	candidates := []*model.Worker{
		{ID: "w1", Status: model.WorkerHealthy, Available: model.ResourceCapacity{CPU: 1, MemoryMB: 512}},
	}
	w, err := SelectWorker(candidates, model.ResourceRequest{CPU: 1, MemoryMB: 512})
	require.NoError(t, err)
	assert.Equal(t, "w1", w.ID)
}

func TestSelectWorker_PicksLeastLoaded(t *testing.T) {
	candidates := []*model.Worker{
		{ID: "busy", Status: model.WorkerHealthy, RunningJobs: 5, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
		{ID: "idle", Status: model.WorkerHealthy, RunningJobs: 1, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
	}
	w, err := SelectWorker(candidates, model.ResourceRequest{})
	require.NoError(t, err)
	assert.Equal(t, "idle", w.ID)
}

func TestSelectWorker_TieBreaksByRegisteredAt(t *testing.T) {
	now := time.Now()
	candidates := []*model.Worker{
		{ID: "newer", Status: model.WorkerHealthy, RunningJobs: 1, RegisteredAt: now.Add(time.Minute), Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
		{ID: "older", Status: model.WorkerHealthy, RunningJobs: 1, RegisteredAt: now, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
	}
	w, err := SelectWorker(candidates, model.ResourceRequest{})
	require.NoError(t, err)
	assert.Equal(t, "older", w.ID)
}

func TestSelectWorker_SkipsMaxConcurrency(t *testing.T) {
	candidates := []*model.Worker{
		{ID: "full", Status: model.WorkerHealthy, RunningJobs: 2, MaxConcurrent: 2, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
		{ID: "ok", Status: model.WorkerHealthy, RunningJobs: 1, MaxConcurrent: 2, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}},
	}
	w, err := SelectWorker(candidates, model.ResourceRequest{})
	require.NoError(t, err)
	assert.Equal(t, "ok", w.ID)
}
