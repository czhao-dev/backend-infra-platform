package state

import (
	"context"
	"errors"

	"github.com/czhao-dev/control-plane/internal/model"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrAlreadyExists     = errors.New("already exists")
	ErrInvalidTransition = errors.New("invalid state transition")
)

// Store is the control plane's view of desired and observed cluster state:
// workloads (desired state), jobs (execution units), workers (execution
// nodes), and routes (proxy routing config).
type Store interface {
	CreateWorkload(ctx context.Context, w *model.Workload) error
	GetWorkload(ctx context.Context, id string) (*model.Workload, error)
	ListWorkloads(ctx context.Context) ([]*model.Workload, error)
	UpdateWorkload(ctx context.Context, w *model.Workload) error
	TransitionWorkload(ctx context.Context, id string, to model.WorkloadStatus) error
	DeleteWorkload(ctx context.Context, id string) error

	CreateJob(ctx context.Context, j *model.Job) error
	GetJob(ctx context.Context, id string) (*model.Job, error)
	ListJobsByWorkload(ctx context.Context, workloadID string) ([]*model.Job, error)
	ListJobsByStatus(ctx context.Context, status model.JobStatus) ([]*model.Job, error)
	ListJobsByWorker(ctx context.Context, workerID string) ([]*model.Job, error)
	UpdateJob(ctx context.Context, j *model.Job) error
	TransitionJob(ctx context.Context, id string, to model.JobStatus, errMsg string) error

	RegisterWorker(ctx context.Context, w *model.Worker) error
	GetWorker(ctx context.Context, id string) (*model.Worker, error)
	ListWorkers(ctx context.Context) ([]*model.Worker, error)
	UpdateWorker(ctx context.Context, w *model.Worker) error
	TransitionWorker(ctx context.Context, id string, to model.WorkerStatus) error

	UpsertRoute(ctx context.Context, r *model.Route) error
	GetRoute(ctx context.Context, id string) (*model.Route, error)
	ListRoutes(ctx context.Context) ([]*model.Route, error)
	DeleteRoute(ctx context.Context, id string) error
}
