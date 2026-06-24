package reconciler

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

func TestReconciler_UnderstaffedWorkloadCreatesMissingJobs(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{
		ID: "w1", Status: model.WorkloadPending, Replicas: 3, Command: "echo",
	}))

	rc := New(st, time.Hour, time.Hour, testLogger())
	rc.Tick(ctx)

	wl, err := st.GetWorkload(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, model.WorkloadActive, wl.Status, "reconciler activates a PENDING workload on first tick")

	jobs, _ := st.ListJobsByWorkload(ctx, "w1")
	assert.Len(t, jobs, 3)
	for _, j := range jobs {
		assert.Equal(t, model.JobPending, j.Status)
		assert.Equal(t, "echo", j.Command)
	}

	// A second tick must not over-create jobs.
	rc.Tick(ctx)
	jobs, _ = st.ListJobsByWorkload(ctx, "w1")
	assert.Len(t, jobs, 3)
}

func TestReconciler_ScaleDownCancelsExcessPendingJobs(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 1}))
	for i := 0; i < 3; i++ {
		require.NoError(t, st.CreateJob(ctx, &model.Job{
			ID: "j" + string(rune('a'+i)), WorkloadID: "w1", Status: model.JobPending,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
		}))
	}

	rc := New(st, time.Hour, time.Hour, testLogger())
	rc.Tick(ctx)

	jobs, _ := st.ListJobsByWorkload(ctx, "w1")
	active := 0
	cancelled := 0
	for _, j := range jobs {
		if j.Active() {
			active++
		}
		if j.Status == model.JobCancelled {
			cancelled++
		}
	}
	assert.Equal(t, 1, active, "scale-down must leave exactly Replicas jobs active")
	assert.Equal(t, 2, cancelled)
}

func TestReconciler_ScaleDownNeverCancelsRunningJobs(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 0}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobRunning, ""))

	rc := New(st, time.Hour, time.Hour, testLogger())
	rc.Tick(ctx)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobRunning, job.Status, "a RUNNING job must never be cancelled by scale-down")
}

func TestReconciler_DeadLetteredJobMarksWorkloadDegraded(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 1}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobRunning, ""))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobFailed, "boom"))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobDeadLetter, ""))

	rc := New(st, time.Hour, time.Hour, testLogger())
	rc.Tick(ctx)

	wl, _ := st.GetWorkload(ctx, "w1")
	assert.Equal(t, model.WorkloadDegraded, wl.Status)

	// A replacement job should have been created to refill the dead-lettered slot.
	jobs, _ := st.ListJobsByWorkload(ctx, "w1")
	assert.Len(t, jobs, 2)
}

func TestReconciler_WorkerHeartbeatTimeoutMarksUnhealthyAndRequeuesRunningJob(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 1, MaxRetries: 2}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", WorkerID: "wk1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobRunning, ""))

	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, LastHeartbeatAt: time.Now().Add(-time.Hour),
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	rc := New(st, time.Hour, 10*time.Second, testLogger())
	rc.Tick(ctx)

	worker, _ := st.GetWorker(ctx, "wk1")
	assert.Equal(t, model.WorkerUnhealthy, worker.Status)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobPending, job.Status, "running job on a timed-out worker is requeued to PENDING")
	assert.Equal(t, 1, job.Attempt)
	assert.True(t, job.RunAfter.After(time.Now()), "requeued job should have a backoff RunAfter set")
}

func TestReconciler_RunningJobExceedingMaxRetriesGoesDeadLetter(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 1, MaxRetries: 0}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", WorkerID: "wk1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobRunning, ""))

	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, LastHeartbeatAt: time.Now().Add(-time.Hour),
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	rc := New(st, time.Hour, 10*time.Second, testLogger())
	rc.Tick(ctx)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobDeadLetter, job.Status, "with MaxRetries=0, a single failure exhausts the budget")
}

func TestReconciler_OrphanedScheduledJobRequeuedWithoutBurningRetry(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.CreateWorkload(ctx, &model.Workload{ID: "w1", Status: model.WorkloadActive, Replicas: 1, MaxRetries: 2}))
	require.NoError(t, st.CreateJob(ctx, &model.Job{ID: "j1", WorkloadID: "w1", WorkerID: "wk1", Status: model.JobPending, CreatedAt: time.Now()}))
	require.NoError(t, st.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	// Worker died between dispatch and pickup: job never reached RUNNING.

	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, LastHeartbeatAt: time.Now().Add(-time.Hour),
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))

	rc := New(st, time.Hour, 10*time.Second, testLogger())
	rc.Tick(ctx)

	job, _ := st.GetJob(ctx, "j1")
	assert.Equal(t, model.JobPending, job.Status, "an orphaned SCHEDULED job must be requeued, not left pointing at a dead worker")
	assert.Equal(t, 0, job.Attempt, "a job that never ran must not burn a retry attempt")
	assert.Equal(t, "", job.WorkerID)
}

func TestReconciler_DrainingWorkerTimeoutBecomesRemoved(t *testing.T) {
	ctx := context.Background()
	st := state.NewMemoryStore()
	require.NoError(t, st.RegisterWorker(ctx, &model.Worker{
		ID: "wk1", Status: model.WorkerRegistering, LastHeartbeatAt: time.Now().Add(-time.Hour),
	}))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerHealthy))
	require.NoError(t, st.TransitionWorker(ctx, "wk1", model.WorkerDraining))

	rc := New(st, time.Hour, 10*time.Second, testLogger())
	rc.Tick(ctx)

	worker, _ := st.GetWorker(ctx, "wk1")
	assert.Equal(t, model.WorkerRemoved, worker.Status, "a draining worker that times out is removed, not marked unhealthy")
}
