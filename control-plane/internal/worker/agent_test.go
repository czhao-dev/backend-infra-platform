package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeControlPlane is a minimal stand-in for the control plane's worker API:
// it hands out one job on the first poll and records reported statuses.
type fakeControlPlane struct {
	mu              sync.Mutex
	registered      bool
	heartbeats      int
	jobServed       int32
	statusesReceived []string
}

func (f *fakeControlPlane) handler(jobToServe *model.Job) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/register":
			f.mu.Lock()
			f.registered = true
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(model.Worker{ID: "wk_test"})

		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/workers/wk_test/heartbeat":
			f.mu.Lock()
			f.heartbeats++
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workers/wk_test/jobs/poll":
			if atomic.CompareAndSwapInt32(&f.jobServed, 0, 1) {
				json.NewEncoder(w).Encode(map[string]any{"job": jobToServe})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"job": nil})
			}

		case r.Method == http.MethodPost && len(r.URL.Path) > len("/api/v1/workers/wk_test/jobs/") &&
			r.URL.Path[len(r.URL.Path)-7:] == "/status":
			var req jobStatusRequest
			json.NewDecoder(r.Body).Decode(&req)
			f.mu.Lock()
			f.statusesReceived = append(f.statusesReceived, string(req.Status))
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func TestAgent_RegistersHeartbeatsAndExecutesJob(t *testing.T) {
	job := &model.Job{ID: "job_1", Command: "echo", Args: []string{"hi"}}
	fake := &fakeControlPlane{}
	srv := httptest.NewServer(fake.handler(job))
	defer srv.Close()

	agent := New(Config{
		ControlPlaneURL:   srv.URL,
		Hostname:          "test",
		Address:           "http://test:9100",
		MaxConcurrentJobs: 2,
		HeartbeatInterval: 20 * time.Millisecond,
		PollInterval:      10 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
	}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()

	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.statusesReceived) >= 2 // RUNNING then SUCCEEDED
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.heartbeats > 0
	}, 2*time.Second, 10*time.Millisecond)

	fake.mu.Lock()
	assert.True(t, fake.registered)
	assert.Contains(t, fake.statusesReceived, string(model.JobRunning))
	assert.Contains(t, fake.statusesReceived, string(model.JobSucceeded))
	fake.mu.Unlock()

	cancel()
	require.NoError(t, <-done)
}

func TestAgent_GracefulShutdownDrainsInFlightJob(t *testing.T) {
	job := &model.Job{ID: "job_slow", Command: "sleep", Args: []string{"0.3"}}
	fake := &fakeControlPlane{}
	srv := httptest.NewServer(fake.handler(job))
	defer srv.Close()

	agent := New(Config{
		ControlPlaneURL:   srv.URL,
		MaxConcurrentJobs: 1,
		HeartbeatInterval: time.Hour,
		PollInterval:      10 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
	}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()

	// Let the agent register and start running the job (reports RUNNING),
	// then immediately request shutdown -- the in-flight "sleep 0.3" job
	// should still be allowed to finish and report SUCCEEDED before Run()
	// returns.
	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for _, s := range fake.statusesReceived {
			if s == string(model.JobRunning) {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Contains(t, fake.statusesReceived, string(model.JobSucceeded), "in-flight job must be allowed to finish during graceful shutdown")
}
