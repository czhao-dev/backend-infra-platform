# distributed-job-platform

A distributed ML job platform: a Go reverse proxy/load balancer fronting multiple replicas of an ML job orchestrator, with Prometheus + Grafana observability across the whole stack.

```
                 ┌──────────┐
   client  ───▶  │  proxy   │  round-robin, active health checks, retry/failover
                 │  :8080   │
                 └────┬─────┘
            ┌──────────┼──────────┐
            ▼          ▼          ▼
     orchestrator-1 orchestrator-2 orchestrator-3   (each :8080 internally)
            │          │          │
            └──────────┴──────────┘
                       │
              ┌────────┴────────┐
              ▼                 ▼
        Prometheus :9090   Grafana :3000
```

This repo combines two standalone projects, each still buildable/runnable on its own:

- [`ml-job-orchestrator/`](ml-job-orchestrator/README.md) — REST API job scheduler: worker pool, subprocess executor, retry/backoff, in-memory state store, Prometheus metrics.
- [`reverse-proxy-load-balancer/`](reverse-proxy-load-balancer/README.md) — reverse proxy / load balancer: round-robin/least-conn/weighted strategies, active health checking, retry/failover.

See each project's own README for implementation details, design tradeoffs, and API references — this README only covers how they fit together.

## Quickstart

```bash
docker compose up --build
```

This builds and starts 3 orchestrator replicas, the proxy in front of them, Prometheus, and Grafana. Give it ~5-10 seconds after startup for the proxy's active health checker to mark all 3 replicas healthy before traffic looks fully balanced — there are no container-level health checks here; the proxy's own health-check loop does that job.

- **Proxy (entry point)**: http://localhost:8080
- **Proxy backend status**: http://localhost:8080/admin/backends
- **Prometheus**: http://localhost:9090
- **Grafana** (admin/admin): http://localhost:3000

Submit a job through the proxy:

```bash
curl -X POST localhost:8080/jobs -d '{"type":"training","command":"sleep 2"}'
```

## Known limitation: per-replica state

Each orchestrator replica keeps its own independent in-memory job store — there is no shared backing store across replicas. Because the proxy load-balances round-robin, a job submitted via `POST /jobs` might land on `orchestrator-2`, but a later `GET /jobs/{id}` or `DELETE /jobs/{id}` can be routed to `orchestrator-1` or `orchestrator-3` and return `404`, since that replica never saw the job. This is a direct consequence of pairing a stateless load balancer with stateful in-memory replicas, not a bug. Fixing it properly would mean either sticky routing by job ID (consistent hashing) or a shared backing store (e.g. Redis) for job state — both are reasonable extensions but out of scope here.

Relatedly, the proxy's health check (`interval: 2s`, `unhealthy_threshold: 2`) takes up to ~4s to notice a replica has started shutting down, so this isn't a production-grade rolling-restart story either — fine for a demo, worth knowing if you script against it.

## mlctl

`ml-job-orchestrator`'s CLI (`mlctl`) defaults to `MLCTL_SERVER=http://localhost:8080`, which now points at the proxy rather than a single orchestrator — that's the intended story, since the proxy is the platform's single entry point. To talk to one specific replica directly (e.g. for debugging), use `docker compose exec` into that container, or temporarily add a host port mapping for it in `docker-compose.yml`.

## Local Go development

A root [`go.work`](go.work) workspace references both modules so editors/tools can resolve `go build ./...`, `go vet ./...`, etc. from the repo root without juggling two separate module roots. The two services remain separate Go modules with independent `go.mod` files — `go.work` is purely a dev convenience and doesn't change either module's dependencies. Note that its presence also activates workspace mode for builds run from *inside* either subdirectory, not just from root; harmless here since neither module uses `replace` directives.

```bash
go build ./...
go vet ./...
go test ./...
```

## Standalone use

Each subdirectory is still a fully independent project — its own `Dockerfile`, `docker-compose.yml`, `go.mod`, and `LICENSE` are untouched, so either one can be extracted and run/cloned on its own:

```bash
cd ml-job-orchestrator && docker compose up --build      # orchestrator + prometheus + grafana alone
cd reverse-proxy-load-balancer && docker compose up --build   # proxy + its own demo backends alone
```
