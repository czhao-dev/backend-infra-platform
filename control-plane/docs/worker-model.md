# Worker model

There are two distinct things named "worker" in this codebase — worth disambiguating up front:

- `internal/model.Worker` — the control plane's **server-side record** of a registered worker: ID, address, capacity, status, last heartbeat. Pure data, lives in the state store.
- `internal/worker.Agent` — the **client-side process** (`cmd/worker`) that registers, heartbeats, polls for jobs, and executes them. This doc is about the agent.

## Lifecycle

```
start worker
  -> register with control plane (capacity, address) -> gets a Worker ID
  -> heartbeat loop: POST /workers/{id}/heartbeat every 5s (WORKER_HEARTBEAT_INTERVAL)
  -> poll loop: GET /workers/{id}/jobs/poll every 1s (WORKER_POLL_INTERVAL)
       -> on a job: acquire a concurrency-semaphore slot, run it as a subprocess
       -> report RUNNING, then SUCCEEDED/FAILED/CANCELLED, via POST .../status
  -> on SIGTERM: stop polling immediately, let in-flight jobs finish
       (up to WORKER_SHUTDOWN_TIMEOUT, default 10s), then cancel stragglers
```

Worker identity is **not persisted** across restarts — a worker that's killed and restarted registers fresh and gets a brand-new ID. The control plane never reconciles "this is actually the same physical worker as before"; it's simply a new entry in the store, and the old ID stays `UNHEALTHY` forever (see the reconciler's known gap).

## Subprocess execution

`internal/worker/executor.go` is a fresh implementation (not a cross-module import) that pattern-matches `ml-job-orchestrator/internal/executor`: `exec.CommandContext`, output captured into a `bytes.Buffer`, exit code extracted via `*exec.ExitError`. It deliberately does **not** import `ml-job-orchestrator` — the two modules stay independent per this repo's structural decision (see the root README).

## Graceful shutdown

The agent's job-execution context (`runCtx` in `internal/worker/agent.go`) is **not** derived from the process's cancellation context — if it were, an in-flight job would be killed the instant `SIGTERM` arrives, defeating the point of draining. Instead, `runCtx` is only cancelled by the shutdown-timeout escape hatch (mirroring `ml-job-orchestrator/internal/worker/pool.go`'s `Shutdown`). This was a real bug caught by `TestAgent_GracefulShutdownDrainsInFlightJob` during development, not a hypothetical.

## Concurrency

A buffered channel (`chan struct{}`, capacity `WORKER_MAX_CONCURRENT_JOBS`) gates how many jobs run at once — simpler than a full goroutine-pool since each worker self-throttles via polling rather than consuming from a shared dispatch channel.

## Metrics and HTTP listener

The worker isn't a job-serving HTTP service — jobs run as subprocesses, not as requests it handles. Its only HTTP listener (`WORKER_METRICS_PORT`, default 9100) exists purely for `/healthz`, `/readyz`, and `/metrics` — for Prometheus scraping, container healthchecks, and (in the proxy-failover demo) as the address the dynamic-discovery proxy forwards traffic to. Worker metrics (`worker_running_jobs`, `worker_completed_jobs_total`, etc.) live in `internal/agentmetrics`, a package separate from the control plane's `internal/metrics` — see that package's doc comment for why splitting it mattered (otherwise every worker's `/metrics` would also expose always-zero control-plane metrics, and vice versa, since both binaries would be importing the same package).
