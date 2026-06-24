package state

import (
	"context"
	"sync"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
)

// MemoryStore is a concurrent, in-memory Store backed by sync.Map, one per
// entity type. A single coarse mutex guards all write paths: this is a
// low-throughput control plane (not a hot data path), and a shared mutex
// keeps cross-entity invariants (e.g. the reconciler counting jobs per
// workload) simple to reason about without per-map lock ordering. Reads are
// lock-free.
type MemoryStore struct {
	mu        sync.Mutex
	workloads sync.Map // id -> model.Workload
	jobs      sync.Map // id -> model.Job
	workers   sync.Map // id -> model.Worker
	routes    sync.Map // id -> model.Route
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

var _ Store = (*MemoryStore)(nil)

// --- Workloads ---

func (s *MemoryStore) CreateWorkload(_ context.Context, w *model.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workloads.Load(w.ID); exists {
		return ErrAlreadyExists
	}
	s.workloads.Store(w.ID, w.Clone())
	return nil
}

func (s *MemoryStore) GetWorkload(_ context.Context, id string) (*model.Workload, error) {
	v, ok := s.workloads.Load(id)
	if !ok {
		return nil, ErrNotFound
	}
	w := v.(model.Workload).Clone()
	return &w, nil
}

func (s *MemoryStore) ListWorkloads(_ context.Context) ([]*model.Workload, error) {
	var out []*model.Workload
	s.workloads.Range(func(_, v any) bool {
		w := v.(model.Workload).Clone()
		out = append(out, &w)
		return true
	})
	return out, nil
}

func (s *MemoryStore) UpdateWorkload(_ context.Context, w *model.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workloads.Load(w.ID); !exists {
		return ErrNotFound
	}
	w.UpdatedAt = time.Now()
	s.workloads.Store(w.ID, w.Clone())
	return nil
}

func (s *MemoryStore) TransitionWorkload(_ context.Context, id string, to model.WorkloadStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.workloads.Load(id)
	if !ok {
		return ErrNotFound
	}
	w := v.(model.Workload)
	if !model.TransitionWorkload(w.Status, to) {
		return ErrInvalidTransition
	}
	w.Status = to
	w.UpdatedAt = time.Now()
	s.workloads.Store(id, w)
	return nil
}

func (s *MemoryStore) DeleteWorkload(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workloads.Load(id); !exists {
		return ErrNotFound
	}
	s.workloads.Delete(id)
	return nil
}

// --- Jobs ---

func (s *MemoryStore) CreateJob(_ context.Context, j *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs.Load(j.ID); exists {
		return ErrAlreadyExists
	}
	s.jobs.Store(j.ID, j.Clone())
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, id string) (*model.Job, error) {
	v, ok := s.jobs.Load(id)
	if !ok {
		return nil, ErrNotFound
	}
	j := v.(model.Job).Clone()
	return &j, nil
}

func (s *MemoryStore) ListJobsByWorkload(_ context.Context, workloadID string) ([]*model.Job, error) {
	var out []*model.Job
	s.jobs.Range(func(_, v any) bool {
		j := v.(model.Job)
		if j.WorkloadID == workloadID {
			c := j.Clone()
			out = append(out, &c)
		}
		return true
	})
	return out, nil
}

func (s *MemoryStore) ListJobsByStatus(_ context.Context, status model.JobStatus) ([]*model.Job, error) {
	var out []*model.Job
	s.jobs.Range(func(_, v any) bool {
		j := v.(model.Job)
		if j.Status == status {
			c := j.Clone()
			out = append(out, &c)
		}
		return true
	})
	return out, nil
}

func (s *MemoryStore) ListJobsByWorker(_ context.Context, workerID string) ([]*model.Job, error) {
	var out []*model.Job
	s.jobs.Range(func(_, v any) bool {
		j := v.(model.Job)
		if j.WorkerID == workerID {
			c := j.Clone()
			out = append(out, &c)
		}
		return true
	})
	return out, nil
}

// UpdateJob overwrites the stored job wholesale. Callers own validating any
// state-transition rules before calling Update (TransitionJob is the
// validated alternative for state changes).
func (s *MemoryStore) UpdateJob(_ context.Context, j *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs.Load(j.ID); !exists {
		return ErrNotFound
	}
	s.jobs.Store(j.ID, j.Clone())
	return nil
}

// TransitionJob validates and applies a status transition for job id,
// updating ScheduledAt/StartedAt/FinishedAt and Error as part of the same
// atomic update.
func (s *MemoryStore) TransitionJob(_ context.Context, id string, to model.JobStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.jobs.Load(id)
	if !ok {
		return ErrNotFound
	}
	j := v.(model.Job)

	if !model.TransitionJob(j.Status, to) {
		return ErrInvalidTransition
	}

	j.Status = to
	if errMsg != "" {
		j.Error = errMsg
	}

	now := time.Now()
	switch to {
	case model.JobScheduled:
		if j.ScheduledAt == nil {
			j.ScheduledAt = &now
		}
	case model.JobRunning:
		if j.StartedAt == nil {
			j.StartedAt = &now
		}
	}
	if isTerminalJobStatus(to) && j.FinishedAt == nil {
		j.FinishedAt = &now
	}

	s.jobs.Store(id, j)
	return nil
}

func isTerminalJobStatus(s model.JobStatus) bool {
	switch s {
	case model.JobSucceeded, model.JobDeadLetter, model.JobCancelled:
		return true
	default:
		return false
	}
}

// --- Workers ---

func (s *MemoryStore) RegisterWorker(_ context.Context, w *model.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workers.Load(w.ID); exists {
		return ErrAlreadyExists
	}
	s.workers.Store(w.ID, w.Clone())
	return nil
}

func (s *MemoryStore) GetWorker(_ context.Context, id string) (*model.Worker, error) {
	v, ok := s.workers.Load(id)
	if !ok {
		return nil, ErrNotFound
	}
	w := v.(model.Worker).Clone()
	return &w, nil
}

func (s *MemoryStore) ListWorkers(_ context.Context) ([]*model.Worker, error) {
	var out []*model.Worker
	s.workers.Range(func(_, v any) bool {
		w := v.(model.Worker).Clone()
		out = append(out, &w)
		return true
	})
	return out, nil
}

func (s *MemoryStore) UpdateWorker(_ context.Context, w *model.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workers.Load(w.ID); !exists {
		return ErrNotFound
	}
	s.workers.Store(w.ID, w.Clone())
	return nil
}

func (s *MemoryStore) TransitionWorker(_ context.Context, id string, to model.WorkerStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.workers.Load(id)
	if !ok {
		return ErrNotFound
	}
	w := v.(model.Worker)
	if !model.TransitionWorker(w.Status, to) {
		return ErrInvalidTransition
	}
	w.Status = to
	s.workers.Store(id, w)
	return nil
}

// --- Routes ---

func (s *MemoryStore) UpsertRoute(_ context.Context, r *model.Route) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.UpdatedAt = time.Now()
	s.routes.Store(r.ID, r.Clone())
	return nil
}

func (s *MemoryStore) GetRoute(_ context.Context, id string) (*model.Route, error) {
	v, ok := s.routes.Load(id)
	if !ok {
		return nil, ErrNotFound
	}
	r := v.(model.Route).Clone()
	return &r, nil
}

func (s *MemoryStore) ListRoutes(_ context.Context) ([]*model.Route, error) {
	var out []*model.Route
	s.routes.Range(func(_, v any) bool {
		r := v.(model.Route).Clone()
		out = append(out, &r)
		return true
	})
	return out, nil
}

func (s *MemoryStore) DeleteRoute(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.routes.Load(id); !exists {
		return ErrNotFound
	}
	s.routes.Delete(id)
	return nil
}
