# Scheduler

`internal/scheduler` assigns `PENDING` jobs to `HEALTHY` workers on a fixed poll interval (`CTRLPLANE_SCHEDULER_INTERVAL`, default 500ms — same ticker-loop shape as `ml-job-orchestrator`'s scheduler).

## Policy

Each tick (`Scheduler.Tick`):

1. List `PENDING` jobs, sorted FIFO by `CreatedAt`. A job whose `RunAfter` is still in the future (set by the reconciler as a backoff gate after a failure) is skipped until that time passes.
2. List all workers; `placement.SelectWorker` filters to `HEALTHY` ones with enough available CPU/memory and concurrency headroom for the job's workload's `Resources`, then picks the **least-loaded** (lowest `RunningJobs`), breaking ties by earliest `RegisteredAt` for determinism.
3. On a match: assign `WorkerID`, transition the job to `SCHEDULED`, and decrement the worker's `Available` capacity / increment `RunningJobs`.
4. On no match (`ErrNoCapacity`): the job stays `PENDING` and is retried next tick.

Processing jobs in FIFO order satisfies the spec's "FIFO" requirement; the capacity/least-loaded filter inside `SelectWorker` satisfies "resource-aware" — one function does both, since FIFO is about job *order* and resource-awareness is about worker *choice*, and they don't conflict.

## What the scheduler does NOT do

Retry/backoff logic for *failed* jobs lives in the reconciler, not here (see [reconciler.md](reconciler.md)) — the scheduler's only job is "pick up anything that's `PENDING` right now," regardless of *why* it became pending (first attempt, or a reconciler-driven retry).

## Capacity bookkeeping

Capacity is reserved at schedule time (`Available -= Resources`, `RunningJobs++`) and released at job-completion time (`POST .../jobs/{id}/status` handler, or by the reconciler if a worker disappears mid-job) — never adjusted on the intermediate "now running" report, since the slot was already accounted for when the job was dispatched.

## Metrics

`ctrlplane_scheduler_queue_depth` (gauge, jobs still pending after the tick) and `ctrlplane_scheduler_latency_seconds` (histogram, time from `Job.CreatedAt` to being scheduled).
