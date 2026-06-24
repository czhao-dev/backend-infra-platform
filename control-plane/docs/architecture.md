# Architecture

The control plane owns desired state; the data plane (workers, proxy) executes work and serves traffic.

```
Client / infractl
      |
      v
Control Plane API (:7070)
      |
      +--> Workload/Job/Worker/Route Store (in-memory)
      +--> Scheduler        (assigns PENDING jobs to HEALTHY workers)
      +--> Reconciler       (desired-state loop, failure detection)
      |
      v
Worker Agents  <---->  subprocess execution
      |
      v
Reverse Proxy (dynamic-discovery mode)
      |
      +--> polls GET /api/v1/proxy/backends
      +--> routes only to HEALTHY workers
```

## Control plane responsibilities

- Receive `Workload` specs (declarative desired state: command, replicas, retry policy, resources).
- Register `Worker`s and track liveness via heartbeats.
- Schedule `Job`s (one execution unit per workload replica) onto workers with available capacity.
- Reconcile actual job/worker state against desired state every tick: create missing jobs, cancel excess ones, detect heartbeat timeouts, reschedule orphaned work, dead-letter jobs that exhaust their retry budget.
- Expose `Route`/backend data so the reverse proxy can discover and health-route to the live worker fleet.

## Worker agent responsibilities

`cmd/worker` is a separate, independently-running process (see [worker-model.md](worker-model.md)) that:

- Registers with the control plane on startup (capacity, address).
- Heartbeats on a fixed interval.
- Polls for jobs assigned to it (`SCHEDULED` status), executes them as subprocesses, and reports status transitions back.
- Drains in-flight work on shutdown rather than dropping it.

## Why workers are a separate process from the control plane

Unlike `ml-job-orchestrator` (where the worker pool is in-process goroutines inside one binary), here the worker is a standalone binary that can run on a different host, register/deregister independently, and be killed without taking the scheduler down with it — that decoupling is what makes worker-failure recovery (see [reconciler.md](reconciler.md)) a meaningful thing to demonstrate.

## Why the proxy isn't part of this module

`reverse-proxy-load-balancer` is a separate, independently-buildable Go module (see the root README). Rather than duplicating proxy logic here, this module only exposes the HTTP API (`GET /api/v1/proxy/backends`) that proxy's `backend.ConfigProvider` polls — the proxy decides what to do with that data (load-balancing strategy, health checks, retries) entirely on its own.
