# ml-job-orchestrator

> A distributed job scheduler for ML workloads built in Go — REST API for
> job submission, a goroutine-based worker pool for concurrent execution,
> per-job retry logic with exponential backoff, and a Prometheus metrics
> endpoint — deployable as a multi-node cluster with Docker Compose.

---

## Overview

ml-job-orchestrator is a purpose-built task scheduler for machine learning
workloads: training runs, inference jobs, data preprocessing, and evaluation
pipelines. Jobs are submitted via a REST API, queued in a buffered channel,
picked up by a goroutine worker pool, executed as subprocesses, and tracked
through a complete lifecycle with automatic retry on failure.

The project demonstrates the core of what Go was designed for: concurrent
systems where many things happen at once and must be coordinated safely.
Every major Go concurrency primitive — goroutines, channels, `sync.WaitGroup`,
`sync.Mutex`, `context.Context` cancellation — appears naturally in the design
rather than as a forced exercise.

This is the same class of infrastructure that powers production ML platforms:
Argo Workflows, Celery, Ray, and Amazon SageMaker Pipelines all solve
variants of this problem. Building a stripped-down version from scratch
demonstrates you understand the distributed systems concepts those tools
are built on — job queuing, worker lifecycle management, failure recovery,
and observability — which is the knowledge ML infrastructure and backend
platform teams specifically hire for.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Clients                                                        │
│  curl / CLI tool (go run ./cmd/mlctl) / any HTTP client         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  REST API (JSON over HTTP)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  API Server (net/http + gorilla/mux)                            │
│  POST /jobs        submit a new job                             │
│  GET  /jobs/{id}   query job status + result                    │
│  GET  /jobs        list all jobs with optional filters          │
│  DELETE /jobs/{id} cancel a running job                         │
│  GET  /healthz     liveness probe                               │
│  GET  /metrics     Prometheus metrics                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │  enqueue
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Scheduler                                                      │
│  Reads from the pending job store, applies priority ordering,   │
│  writes to the job queue channel                                │
└──────────────────────────┬──────────────────────────────────────┘
                           │  chan Job (buffered)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Worker Pool  (N goroutines, N configured at startup)           │
│  Each worker: range over queue channel, call Executor.Run()     │
│  Cancelled via context.WithCancel on shutdown signal            │
└──────────────────────────┬──────────────────────────────────────┘
                           │  exec.CommandContext
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Job Executor                                                   │
│  Launches the job command as a subprocess, captures stdout/     │
│  stderr, enforces per-job timeout via context deadline,         │
│  updates job state in the State Store on completion or failure  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  writes
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  State Store  (in-memory sync.Map, Redis-backed stretch goal)   │
│  Holds Job structs keyed by ID, updated by workers,             │
│  read by the API server for status queries                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │  reads
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Metrics Collector (prometheus/client_golang)                   │
│  jobs_submitted_total, jobs_completed_total, jobs_failed_total, │
│  job_queue_depth, job_duration_seconds histogram                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Job Lifecycle

A job moves through seven states. Every transition is atomic — the state
field in the `Job` struct is updated under a mutex, and the transition
function validates that the current state permits the requested transition
before applying it.

```
                  ┌──────────┐
    POST /jobs    │ PENDING  │  created, not yet queued
                  └────┬─────┘
                       │ scheduler picks up
                       ▼
                  ┌──────────┐
                  │  QUEUED  │  sitting in the buffered channel
                  └────┬─────┘
                       │ worker dequeues
                       ▼
                  ┌──────────┐
                  │ RUNNING  │  subprocess is executing
                  └────┬─────┘
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
        ┌──────────┐      ┌──────────┐
        │COMPLETED │      │  FAILED  │ exit code ≠ 0 or timeout
        └──────────┘      └────┬─────┘
                               │ retries remaining?
                          ┌────┴────┐
                          │         │
                          ▼         ▼
                      ┌───────┐  ┌──────────┐
                      │PENDING│  │EXHAUSTED │ max retries hit
                      └───────┘  └──────────┘

              DELETE /jobs/{id} from any non-terminal state
                               ▼
                         ┌──────────┐
                         │CANCELLED │
                         └──────────┘
```

Every state transition emits a Prometheus counter increment, making the
lifecycle observable in a Grafana dashboard without any additional
instrumentation.

---

## Key Go Concepts Demonstrated

### Goroutine Worker Pool with a Buffered Channel

The entire concurrency model of the worker pool fits in ~30 lines:

```go
// internal/worker/pool.go
type Pool struct {
    jobQueue chan model.Job
    wg       sync.WaitGroup
    cancel   context.CancelFunc
}

func New(ctx context.Context, numWorkers int, q chan model.Job) *Pool {
    ctx, cancel := context.WithCancel(ctx)
    p := &Pool{jobQueue: q, cancel: cancel}

    for i := 0; i < numWorkers; i++ {
        p.wg.Add(1)
        go func(workerID int) {
            defer p.wg.Done()
            for {
                select {
                case job, ok := <-q:
                    if !ok {
                        return  // channel closed, worker exits
                    }
                    executor.Run(ctx, job)
                case <-ctx.Done():
                    return  // shutdown signal received
                }
            }
        }(i)
    }
    return p
}

func (p *Pool) Shutdown() {
    p.cancel()    // signal all workers to stop
    p.wg.Wait()   // block until all in-flight jobs finish
}
```

The `select` statement is Go's core concurrency primitive — it waits on
multiple channels simultaneously and handles whichever is ready first. The
two cases here represent the two things a worker can do: process a job or
respond to a shutdown signal. This is the idiomatic Go pattern for a
graceful shutdown.

### Context Propagation and Per-Job Timeouts

Every job can specify a `timeout_seconds` field. The executor enforces it
without any manual timer code:

```go
// internal/executor/executor.go
func Run(parentCtx context.Context, job model.Job) {
    ctx := parentCtx
    if job.TimeoutSeconds > 0 {
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(
            parentCtx,
            time.Duration(job.TimeoutSeconds)*time.Second,
        )
        defer cancel()
    }

    cmd := exec.CommandContext(ctx, job.Command, job.Args...)
    cmd.Stdout = &jobOutputBuffer
    cmd.Stderr = &jobOutputBuffer

    if err := cmd.Run(); err != nil {
        if ctx.Err() == context.DeadlineExceeded {
            store.Transition(job.ID, model.StateFailed, "timeout")
        } else {
            store.Transition(job.ID, model.StateFailed, err.Error())
        }
        return
    }
    store.Transition(job.ID, model.StateCompleted, "")
}
```

`exec.CommandContext` automatically sends `SIGKILL` to the subprocess when
the context deadline is exceeded — no manual signal handling required.
Context cancellation propagates down from the shutdown signal to every
running job, so `Shutdown()` terminates runaway processes cleanly.

### Retry with Exponential Backoff

Retry logic is implemented as a simple state transition — when a job fails
and has retries remaining, it is re-inserted into the pending queue with a
computed delay rather than spawning a new goroutine per retry:

```go
func scheduleRetry(job model.Job) {
    job.RetryCount++
    backoff := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second
    backoff = min(backoff, 60*time.Second)   // cap at 60s
    job.RunAfter = time.Now().Add(backoff)
    job.State = model.StatePending
    store.Update(job)
}
```

The scheduler goroutine polls for pending jobs whose `RunAfter` timestamp
has passed, so the retry delay requires no sleeping goroutine.

### Prometheus Metrics

```go
// internal/metrics/metrics.go
var (
    JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "mlorch_jobs_submitted_total",
        Help: "Total jobs submitted, by type",
    }, []string{"job_type"})

    JobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "mlorch_job_duration_seconds",
        Help:    "Job execution duration in seconds",
        Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
    }, []string{"job_type", "state"})

    QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "mlorch_queue_depth",
        Help: "Number of jobs currently queued",
    })
)
```

`promauto` registers metrics automatically — no explicit `Register` call
needed. The histogram buckets for `JobsDuration` are sized for ML jobs
(seconds to tens of minutes) rather than web request latency.

---

## API Reference

### Submit a job
```
POST /jobs
Content-Type: application/json

{
    "name": "train-resnet50",
    "type": "training",
    "command": "python3",
    "args": ["train.py", "--epochs", "10", "--lr", "0.001"],
    "timeout_seconds": 3600,
    "max_retries": 2,
    "priority": 1
}

→ 201 Created
{
    "id": "job_7f3a2c",
    "state": "PENDING",
    "created_at": "2025-06-18T10:00:00Z"
}
```

### Query job status
```
GET /jobs/job_7f3a2c

→ 200 OK
{
    "id": "job_7f3a2c",
    "name": "train-resnet50",
    "state": "COMPLETED",
    "created_at": "2025-06-18T10:00:00Z",
    "started_at": "2025-06-18T10:00:03Z",
    "finished_at": "2025-06-18T10:47:22Z",
    "retry_count": 0,
    "output": "Epoch 10/10 — loss: 0.0312, acc: 0.9891\nModel saved."
}
```

### List jobs with filter
```
GET /jobs?state=FAILED&type=training&limit=20

→ 200 OK
{ "jobs": [...], "total": 3 }
```

### Cancel a running job
```
DELETE /jobs/job_7f3a2c

→ 200 OK
{ "id": "job_7f3a2c", "state": "CANCELLED" }
```

---

## Example Session

```bash
# Submit a training job
$ curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d '{"name":"train-mnist","type":"training",
         "command":"python3","args":["train.py"],
         "timeout_seconds":300,"max_retries":2}' | jq .
{
  "id": "job_a1b2c3",
  "state": "PENDING",
  "created_at": "2025-06-18T10:00:00Z"
}

# Poll until complete
$ watch -n 2 'curl -s http://localhost:8080/jobs/job_a1b2c3 | jq .state'
"QUEUED"
"RUNNING"
"COMPLETED"

# Check metrics
$ curl -s http://localhost:8080/metrics | grep mlorch
mlorch_jobs_submitted_total{job_type="training"} 1
mlorch_jobs_completed_total{job_type="training"} 1
mlorch_job_duration_seconds_bucket{job_type="training",state="COMPLETED",le="300"} 1
mlorch_queue_depth 0

# Use the CLI tool
$ go run ./cmd/mlctl submit --name train-mnist --command python3 \
    --args train.py --timeout 300 --retries 2
Submitted job_a1b2c3

$ go run ./cmd/mlctl status job_a1b2c3
ID:       job_a1b2c3
Name:     train-mnist
State:    COMPLETED
Duration: 4m22s
Retries:  0
```

---

## Repo Structure

```
ml-job-orchestrator/
├── README.md
├── go.mod
├── go.sum
├── Dockerfile
├── docker-compose.yml          ← runs orchestrator + Prometheus + Grafana
├── cmd/
│   ├── orchestrator/
│   │   └── main.go             ← wires everything together, starts server
│   └── mlctl/
│       └── main.go             ← CLI client: submit, status, list, cancel
├── internal/
│   ├── api/
│   │   ├── server.go           ← HTTP server setup, middleware
│   │   └── handlers.go         ← one handler per endpoint
│   ├── model/
│   │   └── job.go              ← Job struct, State enum, transition rules
│   ├── scheduler/
│   │   └── scheduler.go        ← polls state store, writes to queue channel
│   ├── worker/
│   │   └── pool.go             ← goroutine pool, select loop, shutdown
│   ├── executor/
│   │   └── executor.go         ← exec.CommandContext, timeout, output capture
│   ├── store/
│   │   └── store.go            ← sync.Map-based state store, thread-safe ops
│   ├── metrics/
│   │   └── metrics.go          ← Prometheus counter/histogram/gauge defs
│   └── retry/
│       └── retry.go            ← backoff calculation, reschedule logic
├── config/
│   └── config.go               ← env-var based config (port, workers, etc.)
├── prometheus/
│   └── prometheus.yml          ← scrape config for Docker Compose setup
├── grafana/
│   └── dashboard.json          ← pre-built dashboard for job metrics
└── tests/
    ├── api_test.go             ← HTTP handler tests with httptest
    ├── pool_test.go            ← worker pool concurrency tests
    ├── store_test.go           ← state transition tests
    └── integration_test.go     ← submit → run → complete end-to-end
```

---

## Build & Run

```bash
# Dependencies: Go 1.22+, Docker, Docker Compose

# Run locally (single node, no Docker)
go run ./cmd/orchestrator --workers 4 --port 8080

# Run with Docker Compose (orchestrator + Prometheus + Grafana)
docker compose up --build

# Services:
# http://localhost:8080  — orchestrator API
# http://localhost:9090  — Prometheus
# http://localhost:3000  — Grafana (admin/admin)

# Submit a job using the CLI
go run ./cmd/mlctl submit \
    --name "train-resnet" \
    --command python3 \
    --args "train.py --epochs 5" \
    --timeout 600 \
    --retries 3

# Run tests
go test ./...

# Run tests with race detector (detects concurrency bugs)
go test -race ./...

# Check code coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

---

## Step-by-Step Build Guide

### Phase 1 — Core Data Structures (Weekend 1)

**Task 1.1 — Define the Job model**
In `internal/model/job.go`, define the `Job` struct and `State` enum:

```go
type State string

const (
    StatePending   State = "PENDING"
    StateQueued    State = "QUEUED"
    StateRunning   State = "RUNNING"
    StateCompleted State = "COMPLETED"
    StateFailed    State = "FAILED"
    StateExhausted State = "EXHAUSTED"
    StateCancelled State = "CANCELLED"
)

type Job struct {
    ID             string
    Name           string
    Type           string
    Command        string
    Args           []string
    State          State
    Priority       int
    MaxRetries     int
    RetryCount     int
    TimeoutSeconds int
    RunAfter       time.Time
    CreatedAt      time.Time
    StartedAt      *time.Time
    FinishedAt     *time.Time
    Output         string
    ErrorMessage   string
}
```

Implement a `Transition(from, to State) bool` function that validates
allowed state transitions using a map of `State → []State`. Invalid
transitions return false and leave the job state unchanged. Write a
unit test in `tests/store_test.go` covering every valid and invalid
transition. Getting state transitions right before building anything
that modifies them prevents a class of concurrency bugs later.

**Task 1.2 — Implement the state store**
In `internal/store/store.go`, implement a `Store` backed by a `sync.Map`
(Go's built-in concurrent map, safe for simultaneous reads and writes from
multiple goroutines without a mutex):

- `Create(job Job) error` — store a new job, error if ID already exists
- `Get(id string) (Job, error)` — return a copy of the job
- `Update(job Job) error` — overwrite the stored job atomically
- `Transition(id string, to State, errMsg string) error` — get the job,
  call `Transition`, update if valid, error if invalid
- `ListByState(state State) []Job` — iterate the map, filter by state
- `Delete(id string)` — remove a job

Write tests for each method including concurrent reads and writes using
`t.Parallel()`. Run with `go test -race ./...` to confirm no data races.

**Task 1.3 — Add configuration**
In `config/config.go`, define a `Config` struct read from environment
variables using `os.Getenv` with sensible defaults:

```go
type Config struct {
    Port           int           // default 8080
    NumWorkers     int           // default 4
    QueueSize      int           // default 100
    MaxJobHistory  int           // default 1000
    ShutdownTimeout time.Duration // default 30s
}
```

Environment-variable-based config is the twelve-factor app standard and
makes the Docker Compose setup straightforward.

---

### Phase 2 — Worker Pool (Weekend 1, continued)

**Task 2.1 — Implement the worker pool**
In `internal/worker/pool.go`, implement the goroutine pool as shown in
the Key Concepts section. The `jobQueue` parameter is a `chan model.Job`
created by the caller — the pool is a consumer only, it never writes to
the channel. This separation of producer (scheduler) and consumer (pool)
is idiomatic Go channel design.

**Task 2.2 — Implement graceful shutdown**
The `Shutdown()` method must allow in-flight jobs to finish before
returning. Call `p.cancel()` to signal workers to stop accepting new
jobs, then `p.wg.Wait()` to block until all running goroutines exit.
Add a test that submits 10 slow jobs (sleep 1s each) to a 2-worker pool,
calls `Shutdown()`, and asserts all 10 eventually transition to a terminal
state — no jobs left in `RUNNING`.

**Task 2.3 — Test with the race detector**
Write `tests/pool_test.go` that launches a pool with 8 workers and
submits 100 jobs concurrently from 10 goroutines simultaneously. Run with
`go test -race ./internal/worker/...`. A clean run confirms your pool has
no data races. A race condition detected here is a real concurrency bug —
fix it before moving to Phase 3.

---

### Phase 3 — Job Executor (Weekend 2)

**Task 3.1 — Implement the executor**
In `internal/executor/executor.go`, implement the `Run` function as shown
in the Key Concepts section. Key details: capture both stdout and stderr
into a `bytes.Buffer` using `cmd.Stdout` and `cmd.Stderr`, so the full
output is available in the job's `Output` field after execution. Use
`exec.CommandContext` so the process is automatically killed when the
context is cancelled.

**Task 3.2 — Implement retry scheduling**
In `internal/retry/retry.go`, implement the `ScheduleRetry(job Job) Job`
function that increments `RetryCount`, computes the backoff duration
(`2^RetryCount` seconds, capped at 60), sets `RunAfter`, and resets the
state to `PENDING`. The caller checks `job.RetryCount <= job.MaxRetries`
before calling this — if exhausted, transition to `EXHAUSTED` instead.

**Task 3.3 — Test timeout enforcement**
Write a test that submits a job running `sleep 10` with a two-second
timeout. Assert the job transitions to `FAILED` within three seconds
with an error message containing "timeout". This test is the most direct
proof that `context.WithTimeout` and `exec.CommandContext` are wired
together correctly.

---

### Phase 4 — Scheduler (Weekend 2, continued)

**Task 4.1 — Implement the scheduler loop**
In `internal/scheduler/scheduler.go`, implement a goroutine that runs in
a loop polling the state store for `PENDING` jobs whose `RunAfter` is in
the past, sorts them by `Priority` (higher first, then `CreatedAt`), and
writes them to the job queue channel (changing their state to `QUEUED`
atomically before writing):

```go
func (s *Scheduler) Run(ctx context.Context) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            s.scheduleReady()
        case <-ctx.Done():
            return
        }
    }
}
```

The 500ms polling interval is a simple starting point — it means a job
waits at most 500ms in `PENDING` before being queued. Document this
tradeoff in `README.md`: a production scheduler would use a condition
variable or a priority queue with a heap, but polling is simpler to
reason about and sufficient for this scope.

**Task 4.2 — Handle queue backpressure**
The job queue channel is buffered (size from config). When the channel
is full, `scheduleReady` must not block — use a non-blocking send:

```go
select {
case s.queue <- job:
    store.Transition(job.ID, model.StateQueued, "")
default:
    // queue full — leave job in PENDING, try next tick
}
```

This non-blocking select is the idiomatic Go pattern for backpressure
handling. Document it in the code with a comment explaining why a
blocking send here would deadlock the scheduler goroutine.

---

### Phase 5 — REST API Server (Weekend 3)

**Task 5.1 — Implement the HTTP handlers**
In `internal/api/handlers.go`, implement one handler function per
endpoint. Use `encoding/json` for request parsing and response encoding.
Key details: generate a unique job ID with `fmt.Sprintf("job_%s",
generateID())` using a 6-character random hex string; set the `Location`
header on `POST /jobs` to the new job's URL (`/jobs/{id}`); return `404`
with a JSON error body when `store.Get` returns not found.

**Task 5.2 — Implement job cancellation**
`DELETE /jobs/{id}` must cancel a job regardless of which state it is in.
For `RUNNING` jobs, store a `context.CancelFunc` per job ID in a separate
`sync.Map` in the executor, and call it from the handler. For `QUEUED` or
`PENDING` jobs, transition directly to `CANCELLED`. This is the hardest
handler to implement correctly — test it by cancelling a job that has been
running for one second and asserting the subprocess is actually dead with
`ps aux`.

**Task 5.3 — Add middleware**
Implement two middleware functions: a request logger that writes method,
path, status code, and duration to stdout using `log/slog` (Go 1.21's
structured logging package), and a panic recovery middleware that catches
any handler panic, logs the stack trace, and returns a `500` instead of
crashing the server. Wrap all routes with both before starting the server.

**Task 5.4 — Write handler tests**
In `tests/api_test.go`, use `net/http/httptest` to test each handler
without starting a real server:

```go
func TestSubmitJob(t *testing.T) {
    store := store.New()
    h := handlers.New(store, queue)
    body := `{"name":"test","command":"echo","args":["hello"]}`
    req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    w := httptest.NewRecorder()
    h.SubmitJob(w, req)
    assert.Equal(t, http.StatusCreated, w.Code)
}
```

---

### Phase 6 — Metrics & Observability (Weekend 4)

**Task 6.1 — Instrument the critical paths**
Add Prometheus instrumentation at four points: increment
`JobsSubmitted` in the API handler on submit; increment
`JobsCompleted` or `JobsFailed` in the executor on completion; update
`QueueDepth` in the scheduler after each dispatch tick; observe
`JobsDuration` in the executor using `time.Since(job.StartedAt)`.

**Task 6.2 — Set up Docker Compose with Prometheus and Grafana**
Write `docker-compose.yml` that starts three services: the orchestrator
(built from `Dockerfile`), Prometheus (with `prometheus.yml` scraping
`http://orchestrator:8080/metrics` every 5 seconds), and Grafana
(pre-loaded with `grafana/dashboard.json` showing queue depth, throughput,
and job duration percentiles). Run `docker compose up` and confirm the
dashboard shows live data when you submit a batch of jobs.

**Task 6.3 — Create the Grafana dashboard**
Build a dashboard with four panels: a counter rate graph for
`mlorch_jobs_submitted_total`, a gauge for `mlorch_queue_depth`, a
success/failure ratio panel using `jobs_completed / jobs_submitted`, and
a heatmap of `mlorch_job_duration_seconds`. Export the dashboard JSON
to `grafana/dashboard.json` — this is what makes the project immediately
demoable without any manual Grafana configuration.

---

### Phase 7 — CLI Client & Integration Tests (Weekend 4, continued)

**Task 7.1 — Build the mlctl CLI**
In `cmd/mlctl/main.go`, implement a CLI client using the standard
`flag` package (no external dependency needed). Subcommands: `submit`
(flags for name, command, args, timeout, retries, priority), `status`
(takes a job ID, prints a formatted summary), `list` (flags for state
filter and limit), `cancel` (takes a job ID). All subcommands make HTTP
requests to the orchestrator and print formatted output.

**Task 7.2 — Write an end-to-end integration test**
In `tests/integration_test.go`, write a test that starts the orchestrator
in a goroutine, submits five jobs (including one that will fail and one
that will time out), waits for all five to reach a terminal state, and
asserts the correct final states. This test is the most valuable in the
suite — it exercises the entire stack from API to executor to state store
in a single test.

---

### Phase 8 — Polish (Weekend 5)

**Task 8.1 — Add structured logging throughout**
Replace all `fmt.Println` calls with `log/slog` structured log calls,
using consistent field names: `"job_id"`, `"worker_id"`, `"state"`,
`"duration_ms"`. Structured logs are what production systems use — they
are parseable by log aggregation tools (Loki, Splunk, Datadog) without
regex. Include a `--log-level` flag that controls verbosity.

**Task 8.2 — Write a multi-node stretch goal (optional)**
If time permits, add a second mode where multiple orchestrator instances
share a Redis job queue (using `redis/go-redis`) instead of an in-memory
channel. Each instance pulls jobs from Redis, ensuring a job is executed
exactly once across the cluster. This is the step that turns a single-node
scheduler into a genuinely distributed system — worth mentioning as future
work in the README even if you do not implement it.

**Task 8.3 — Document the design decisions**
Add a `docs/design.md` explaining three explicit choices: why
`sync.Map` rather than a `map` with a `sync.RWMutex` for the state
store; why polling in the scheduler rather than a condition variable;
why the job queue is a buffered Go channel rather than Redis in the
base version. Design decision documents signal that you thought about
tradeoffs, not just implementations.

---

## Realistic Timeline

| Phase | Content | Time |
|---|---|---|
| 1 | Job model, state store, config | Weekend 1 |
| 2 | Worker pool + race detector tests | Weekend 1 |
| 3 | Job executor + retry logic | Weekend 2 |
| 4 | Scheduler + backpressure | Weekend 2 |
| 5 | REST API server + handler tests | Weekend 3 |
| 6 | Prometheus metrics + Docker Compose | Weekend 4 |
| 7 | CLI client + integration test | Weekend 4 |
| 8 | Polish + design docs | Weekend 5 |

**Total: ~5 weekends.** The most natural stopping point if time is short
is after Phase 5 — a working REST API with a goroutine worker pool,
retry logic, and state tracking is already a strong and demoable portfolio
project, even before Prometheus and Docker Compose are added.

---

## How to Talk About This Project in an Interview

**What is the project?**
"I built a distributed job scheduler in Go designed for ML workloads —
training runs, inference jobs, preprocessing pipelines. Jobs are submitted
via a REST API, queued in a buffered channel, processed by a goroutine
worker pool, retried on failure with exponential backoff, and tracked
through a complete lifecycle. The whole system is observable via Prometheus
metrics and runs as a multi-service cluster with Docker Compose."

**Walk me through the concurrency model.**
"The worker pool is a fixed number of goroutines, each running a `select`
loop over two channels: the job queue and a shutdown signal. When a job
arrives on the queue channel, the worker calls the executor. When the
shutdown context is cancelled, the worker finishes its current job and
exits. A `sync.WaitGroup` in the pool's `Shutdown` method blocks until
all workers have returned. The whole thing is maybe 30 lines of Go but
correctly handles every lifecycle: normal execution, job failure, timeout,
and graceful shutdown."

**How does job cancellation work for a running job?**
"When the API receives a `DELETE /jobs/{id}` request for a running job,
it looks up a per-job `context.CancelFunc` stored in a concurrent map,
calls it, which cancels the context passed to `exec.CommandContext`. Go's
`exec.CommandContext` sends `SIGKILL` to the subprocess automatically
when the context is cancelled — no manual signal handling required. The
executor's error path detects the cancellation and transitions the job to
`CANCELLED` in the state store."

**What would you do differently at production scale?**
"Three things. The in-memory state store would be replaced with Redis or
PostgreSQL for durability — right now a restart loses all job state. The
polling scheduler would be replaced with a heap-based priority queue and
a condition variable to avoid the 500ms dispatch latency. And the single
node would become a cluster, where multiple orchestrator instances pull
from a shared Redis queue with a distributed lock to guarantee each job
runs exactly once."

---

## Further Reading

- [The Go Programming Language — Donovan & Kernighan](https://www.gopl.io/)
  — Chapters 8 and 9 cover goroutines, channels, and concurrency in depth;
  the worker pool pattern in this project is a direct application of those
  chapters
- [Go Concurrency Patterns — Rob Pike (talk)](https://go.dev/talks/2012/concurrency.slide)
  — the canonical presentation of the select, fan-out, and pipeline patterns
- [prometheus/client_golang](https://github.com/prometheus/client_golang)
  — the official Go Prometheus client used in this project
- [The Twelve-Factor App](https://12factor.net/)
  — the methodology behind the environment-variable configuration and
  stateless worker design in this project
- [Argo Workflows](https://argoproj.github.io/argo-workflows/)
  — the production ML job orchestrator this project is a simplified version
  of; comparing designs is worth a section in your README
