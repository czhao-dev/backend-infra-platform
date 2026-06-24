package scheduler

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestScheduler_AssignsPendingJobToHealthyWorker(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()

	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{
		ID: "w1", Resources: model.ResourceRequest{CPU: 1, MemoryMB: 256},
	}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, Capacity: model.ResourceCapacity{CPU: 2, MemoryMB: 1024}, Available: model.ResourceCapacity{CPU: 2, MemoryMB: 1024},
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	sched := New(st, time.Hour, testLogger())
	sched.Tick(ctx)

	job, err := st.GetJob(ctx, "j1")
	require.NoError(t, err)
	assert.Equal(t, model.JobScheduled, job.Status)
	assert.Equal(t, "wk1", job.WorkerID)

	worker, _ := st.GetWorker(ctx, "wk1")
	assert.Equal(t, 1, worker.RunningJobs)
	assert.Equal(t, 1.0, worker.Available.CPU)
	assert.Equal(t, 768, worker.Available.MemoryMB)

	stats := sched.Stats()
	assert.Equal(t, 1, stats.ScheduledLast)
	assert.Equal(t, 0, stats.PendingNow)
}

func TestScheduler_LeavesJobPendingWhenNoCapacity(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()

	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Resources: model.ResourceRequest{CPU: 4, MemoryMB: 4096}}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, Capacity: model.ResourceCapacity{CPU: 1, MemoryMB: 512}, Available: model.ResourceCapacity{CPU: 1, MemoryMB: 512},
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	sched := New(st, time.Hour, testLogger())
	sched.Tick(ctx)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobPending, job.Status)
}

func TestScheduler_RespectsRunAfterBackoffGate(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()

	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1"}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{
		ID: "j1", WorkloadID: "w1", Status: model.JobPending, CreatedAt: time.Now(),
		RunAfter: time.Now().Add(time.Hour),
	}))
	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{ID: "wk1", Status: model.WorkerRegistering, Available: model.ResourceCapacity{CPU: 10, MemoryMB: 10000}}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	sched := New(st, time.Hour, testLogger())
	sched.Tick(ctx)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobPending, job.Status, "job with future RunAfter must not be scheduled yet")
}
