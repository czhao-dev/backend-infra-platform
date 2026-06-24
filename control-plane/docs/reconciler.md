# Reconciler

`internal/reconciler` maintains desired state under failure on a fixed interval (`CTRLPLANE_RECONCILE_INTERVAL`, default 2s). Each tick runs two independent passes.

## Pass 1: replica reconciliation

For every non-`CANCELLED` workload:

- A `PENDING` workload is activated (`ACTIVE`) the first time the reconciler sees it.
- `active` = count of jobs whose status counts toward a replica slot (`Job.Active()`: `PENDING`/`SCHEDULED`/`RUNNING`/`RETRYING`/`FAILED`/`SUCCEEDED` — only `DEAD_LETTER` and `CANCELLED` free a slot). This treats `Replicas` as "run this many job instances" (batch semantics), not "keep N running forever" — see the comment on `Job.Active()` for why, and for the documented gap around `restart_policy: always`.
- If `active < Replicas`: create the missing jobs as `PENDING`, copying the workload's `Command`/`Args`.
- If `active > Replicas` (scale-down): cancel the newest excess `PENDING`/`SCHEDULED` jobs first. **A `RUNNING` job is never cancelled by scale-down** — it's left to finish.
- If any job for the workload is `DEAD_LETTER`, the workload is marked `DEGRADED` (and back to `ACTIVE` once no dead-lettered jobs remain) — a cheap signal that something needed manual attention.

## Pass 2: worker heartbeat timeout detection

For every worker whose `now - LastHeartbeatAt` exceeds `CTRLPLANE_HEARTBEAT_TIMEOUT` (default 15s, ~3x the worker's own 5s heartbeat interval):

- `HEALTHY → UNHEALTHY`, then reschedule its jobs (see below).
- `DRAINING → REMOVED` instead — an operator-initiated decommission (`infractl worker drain`) that's gone quiet is treated as a completed removal, not a failure.

A worker recovers (`UNHEALTHY → HEALTHY`) the instant it heartbeats again — that direction is owned by the heartbeat HTTP handler, not the reconciler, since receiving a heartbeat is definitionally proof of life. **Known gap:** a worker that crashes and never sends another heartbeat stays `UNHEALTHY` forever; there's no automatic garbage-collection for permanently-dead workers (only the `DRAINING → REMOVED` path fully removes one).

### Rescheduling a failed worker's jobs

- A job that was `SCHEDULED` but never reached `RUNNING` (the worker died between dispatch and pickup) is sent straight back to `PENDING` — it never executed, so it doesn't burn a retry attempt.
- A job that was actually `RUNNING` is pessimistically assumed lost: `Attempt++`, and if `Attempt <= Workload.MaxRetries`, it's requeued to `PENDING` with an exponential-backoff `RunAfter` (`2^attempt` seconds, capped at 60s — same formula as `ml-job-orchestrator/internal/retry`). If the retry budget is exhausted, it goes to `DEAD_LETTER` instead.

This two-case split was found by actually running the worker-failure demo against a live cluster — an earlier version only handled the `RUNNING` case, leaving `SCHEDULED` jobs permanently stuck pointing at a dead worker (see `TestReconciler_OrphanedScheduledJobRequeuedWithoutBurningRetry`).
