package state

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func newTestWorkload(id string) *model.Workload {
	return &model.Workload{ID: id, Name: "test", Status: model.WorkloadPending, Replicas: 1}
}

func newTestJob(id, workloadID string) *model.Job {
	return &model.Job{ID: id, WorkloadID: workloadID, Status: model.JobPending}
}

func newTestWorker(id string) *model.Worker {
	return &model.Worker{ID: id, Status: model.WorkerRegistering}
}

func TestWorkloadCRUD(t *testing.T) {
	s := NewMemoryStore()
	w := newTestWorkload("w1")

	require.NoError(t, s.CreateWorkload(ctx, w))
	require.ErrorIs(t, s.CreateWorkload(ctx, w), ErrAlreadyExists)

	got, err := s.GetWorkload(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, "test", got.Name)

	_, err = s.GetWorkload(ctx, "missing")
	require.ErrorIs(t, err, ErrNotFound)

	got.Name = "renamed"
	require.NoError(t, s.UpdateWorkload(ctx, got))
	got2, _ := s.GetWorkload(ctx, "w1")
	assert.Equal(t, "renamed", got2.Name)

	require.NoError(t, s.TransitionWorkload(ctx, "w1", model.WorkloadActive))
	require.ErrorIs(t, s.TransitionWorkload(ctx, "w1", model.WorkloadPending), ErrInvalidTransition)

	list, err := s.ListWorkloads(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	require.NoError(t, s.DeleteWorkload(ctx, "w1"))
	require.ErrorIs(t, s.DeleteWorkload(ctx, "w1"), ErrNotFound)
}

func TestJobCRUDAndTransition(t *testing.T) {
	s := NewMemoryStore()
	j := newTestJob("j1", "w1")
	require.NoError(t, s.CreateJob(ctx, j))
	require.ErrorIs(t, s.CreateJob(ctx, j), ErrAlreadyExists)

	require.NoError(t, s.TransitionJob(ctx, "j1", model.JobScheduled, ""))
	got, err := s.GetJob(ctx, "j1")
	require.NoError(t, err)
	require.NotNil(t, got.ScheduledAt)

	require.NoError(t, s.TransitionJob(ctx, "j1", model.JobRunning, ""))
	got, _ = s.GetJob(ctx, "j1")
	require.NotNil(t, got.StartedAt)

	require.ErrorIs(t, s.TransitionJob(ctx, "j1", model.JobDeadLetter, ""), ErrInvalidTransition)

	require.NoError(t, s.TransitionJob(ctx, "j1", model.JobFailed, "boom"))
	got, _ = s.GetJob(ctx, "j1")
	assert.Equal(t, "boom", got.Error)
	assert.Nil(t, got.FinishedAt, "FAILED is not terminal -- reconciler still decides retry vs dead-letter")

	require.NoError(t, s.TransitionJob(ctx, "j1", model.JobDeadLetter, ""))
	got, _ = s.GetJob(ctx, "j1")
	require.NotNil(t, got.FinishedAt)

	require.ErrorIs(t, s.TransitionJob(ctx, "missing", model.JobPending, ""), ErrNotFound)
}

func TestJobListByWorkloadStatusWorker(t *testing.T) {
	s := NewMemoryStore()
	for i := 0; i < 3; i++ {
		j := newTestJob(fmt.Sprintf("j%d", i), "w1")
		j.WorkerID = "worker_x"
		require.NoError(t, s.CreateJob(ctx, j))
	}
	other := newTestJob("j_other", "w2")
	require.NoError(t, s.CreateJob(ctx, other))
	require.NoError(t, s.TransitionJob(ctx, "j_other", model.JobScheduled, ""))

	byWorkload, _ := s.ListJobsByWorkload(ctx, "w1")
	assert.Len(t, byWorkload, 3)

	byStatus, _ := s.ListJobsByStatus(ctx, model.JobPending)
	assert.Len(t, byStatus, 3)

	byWorker, _ := s.ListJobsByWorker(ctx, "worker_x")
	assert.Len(t, byWorker, 3)
}

func TestWorkerRegisterAndTransition(t *testing.T) {
	s := NewMemoryStore()
	w := newTestWorker("wk1")
	require.NoError(t, s.RegisterWorker(ctx, w))
	require.ErrorIs(t, s.RegisterWorker(ctx, w), ErrAlreadyExists)

	require.NoError(t, s.TransitionWorker(ctx, "wk1", model.WorkerHealthy))
	got, err := s.GetWorker(ctx, "wk1")
	require.NoError(t, err)
	assert.Equal(t, model.WorkerHealthy, got.Status)

	require.ErrorIs(t, s.TransitionWorker(ctx, "wk1", model.WorkerRemoved), ErrInvalidTransition)

	list, _ := s.ListWorkers(ctx)
	assert.Len(t, list, 1)
}

func TestRouteCRUD(t *testing.T) {
	s := NewMemoryStore()
	r := &model.Route{ID: "r1", Name: "test", PathPrefix: "/x"}
	require.NoError(t, s.UpsertRoute(ctx, r))

	got, err := s.GetRoute(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "/x", got.PathPrefix)

	got.PathPrefix = "/y"
	require.NoError(t, s.UpsertRoute(ctx, got))
	got2, _ := s.GetRoute(ctx, "r1")
	assert.Equal(t, "/y", got2.PathPrefix)

	list, _ := s.ListRoutes(ctx)
	assert.Len(t, list, 1)

	require.NoError(t, s.DeleteRoute(ctx, "r1"))
	require.ErrorIs(t, s.DeleteRoute(ctx, "r1"), ErrNotFound)
}

// TestStoreConcurrentAccess exercises Create/Get/Transition from many
// goroutines simultaneously against disjoint and shared IDs. Run with
// `go test -race` to confirm no data races.
func TestStoreConcurrentAccess(t *testing.T) {
	s := NewMemoryStore()
	const numGoroutines = 20
	const numJobsEach = 25

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numJobsEach; i++ {
				id := fmt.Sprintf("g%d_job%d", g, i)
				job := newTestJob(id, "w1")
				if err := s.CreateJob(ctx, job); err != nil {
					t.Errorf("CreateJob(%s): %v", id, err)
					continue
				}
				if err := s.TransitionJob(ctx, id, model.JobScheduled, ""); err != nil {
					t.Errorf("TransitionJob(%s): %v", id, err)
				}
			}
		}()
	}
	wg.Wait()

	jobs, _ := s.ListJobsByStatus(ctx, model.JobScheduled)
	assert.Len(t, jobs, numGoroutines*numJobsEach)

	// Hammer a single shared job concurrently to confirm Transition serializes.
	require.NoError(t, s.CreateJob(ctx, newTestJob("shared", "w1")))
	require.NoError(t, s.TransitionJob(ctx, "shared", model.JobScheduled, ""))

	var wg2 sync.WaitGroup
	successes := make([]bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		g := g
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			err := s.TransitionJob(ctx, "shared", model.JobRunning, "")
			successes[g] = err == nil
		}()
	}
	wg2.Wait()

	successCount := 0
	for _, ok := range successes {
		if ok {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "exactly one goroutine should win the SCHEDULED->RUNNING transition")
}
