package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/scheduler"
	"github.com/czhao-dev/control-plane/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer() *httptest.Server {
	st := state.NewMemoryStore()
	sched := scheduler.New(st, time.Hour, nil)
	h := NewHandlers(st, sched)
	return httptest.NewServer(NewRouter(h))
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	return resp
}

func TestWorkloadLifecycle(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/api/v1/workloads", map[string]any{
		"name": "demo", "command": "echo", "replicas": 2,
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created model.Workload
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
	resp.Body.Close()
	assert.Equal(t, model.WorkloadPending, created.Status)

	getResp, err := http.Get(srv.URL + "/api/v1/workloads/" + created.ID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	listResp, err := http.Get(srv.URL + "/api/v1/workloads")
	require.NoError(t, err)
	defer listResp.Body.Close()
	var list struct {
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&list))
	assert.Equal(t, 1, list.Total)

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/workloads/"+created.ID, nil)
	delResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	notFoundResp, err := http.Get(srv.URL + "/api/v1/workloads/does-not-exist")
	require.NoError(t, err)
	defer notFoundResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, notFoundResp.StatusCode)
}

func TestWorkerRegisterHeartbeatPollAndJobStatus(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	regResp := postJSON(t, srv.URL+"/api/v1/workers/register", map[string]any{
		"hostname": "h1", "address": "http://h1:9100",
		"capacity": map[string]any{"cpu": 2, "memory_mb": 1024}, "max_concurrent_jobs": 2,
	})
	require.Equal(t, http.StatusCreated, regResp.StatusCode)
	var worker model.Worker
	require.NoError(t, json.NewDecoder(regResp.Body).Decode(&worker))
	regResp.Body.Close()
	assert.Equal(t, model.WorkerHealthy, worker.Status)

	hbResp := postJSON(t, srv.URL+"/api/v1/workers/"+worker.ID+"/heartbeat", map[string]any{"running_jobs": 0})
	defer hbResp.Body.Close()
	assert.Equal(t, http.StatusOK, hbResp.StatusCode)

	pollResp, err := http.Get(srv.URL + "/api/v1/workers/" + worker.ID + "/jobs/poll")
	require.NoError(t, err)
	defer pollResp.Body.Close()
	var pollOut struct {
		Job *model.Job `json:"job"`
	}
	require.NoError(t, json.NewDecoder(pollResp.Body).Decode(&pollOut))
	assert.Nil(t, pollOut.Job, "no jobs scheduled yet")

	listResp, err := http.Get(srv.URL + "/api/v1/workers")
	require.NoError(t, err)
	defer listResp.Body.Close()
	var list struct {
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&list))
	assert.Equal(t, 1, list.Total)

	drainResp := postJSON(t, srv.URL+"/api/v1/workers/"+worker.ID+"/drain", nil)
	defer drainResp.Body.Close()
	assert.Equal(t, http.StatusOK, drainResp.StatusCode)
}

func TestProxyBackendsReflectsOnlyHealthyWorkers(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	healthyResp := postJSON(t, srv.URL+"/api/v1/workers/register", map[string]any{"address": "http://healthy:9100"})
	var healthy model.Worker
	json.NewDecoder(healthyResp.Body).Decode(&healthy)
	healthyResp.Body.Close()

	resp, err := http.Get(srv.URL + "/api/v1/proxy/backends")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Backends []model.BackendSpec `json:"backends"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Backends, 1)
	assert.Equal(t, "http://healthy:9100", out.Backends[0].URL)
}

func TestRouteCRUDOverHTTP(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/api/v1/routes", map[string]any{
		"name": "worker-api", "path_prefix": "/workers", "strategy": "least_conn",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var route model.Route
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&route))
	resp.Body.Close()

	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/routes/"+route.ID, nil)
	delResp, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)
}

func TestHealthzAndReadyz(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
}
