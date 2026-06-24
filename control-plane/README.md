# control-plane

A mini infrastructure control plane: declarative workload scheduling, worker registration/heartbeats, desired-state reconciliation, failure recovery, and a backend-discovery API for health-aware reverse-proxy routing.

This is one of three independent Go modules in the parent [distributed-job-platform](../README.md) repo — see that README for how it fits together with the original `ml-job-orchestrator`/`reverse-proxy-load-balancer` stack, and [docs/architecture.md](docs/architecture.md) for the design.

## Binaries

- `cmd/control-plane` — the control plane API server: workload/worker/route store, scheduler loop, reconciler loop.
- `cmd/worker` — the worker agent: registers, heartbeats, polls for jobs, executes them as subprocesses.
- `cmd/infractl` — stdlib-only CLI client for the control plane's REST API.

## Quickstart (no Docker)

```bash
go run ./cmd/control-plane &
go run ./cmd/worker &
go run ./cmd/worker &   # a second worker, registers independently

go run ./cmd/infractl workload submit examples/batch-job.yaml
go run ./cmd/infractl worker list
go run ./cmd/infractl workload status <workload-id>
```

`INFRACTL_SERVER` (default `http://localhost:7070`) points `infractl` at a different control plane. See [../docker-compose.yml](../docker-compose.yml) and [scripts/](scripts/) for the full Docker-based demo (multiple workers, dynamic-discovery proxy, Prometheus/Grafana).

## Domain model

`Workload` (desired state: command, replicas, retry policy, resources) → `Job` (one execution unit per replica) → assigned to a `Worker` by the scheduler. `Route`/`BackendSpec` are config data the reverse proxy discovers via `GET /api/v1/proxy/backends`. All four follow the same `State string` + `allowedTransitions map[State][]State` + pure `Transition(from, to) bool` validator pattern (see `internal/model`), applied inside the state store's mutex-guarded write path.

## Testing

```bash
go test ./... -race
```

Unit tests cover model state-transition matrices, the in-memory store (including concurrent-access races), scheduler placement logic, reconciler failure scenarios, the HTTP API (via `httptest`), and the worker agent (registration/heartbeat/poll/execute/graceful-shutdown against a fake control-plane server). The reconciler and worker-agent test suites each caught a real bug during development — see [docs/reconciler.md](docs/reconciler.md) and [docs/worker-model.md](docs/worker-model.md).

## Docs

- [docs/architecture.md](docs/architecture.md) — control-plane/data-plane overview
- [docs/scheduler.md](docs/scheduler.md) — placement policy
- [docs/reconciler.md](docs/reconciler.md) — desired-state reconciliation and failure recovery
- [docs/worker-model.md](docs/worker-model.md) — worker agent lifecycle

## Non-goals

In-memory state only (no persistence layer yet — see `internal/state.Store`, designed so a SQLite/BoltDB implementation could be added without touching callers). No priority scheduling, preemption, autoscaling, leader election, or distributed consensus. See the parent repo's `control_plane.md` for the full spec this module implements.
