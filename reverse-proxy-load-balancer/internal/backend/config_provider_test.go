package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestConfigProvider_FetchesAndSwapsBackends(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"backends": []map[string]any{
				{"name": "b1", "url": "http://localhost:9001", "weight": 1},
				{"name": "b2", "url": "http://localhost:9002", "weight": 2},
			},
		})
	}))
	defer srv.Close()

	pool := NewPool(nil)
	provider := NewConfigProvider(pool, srv.URL, time.Hour, testLogger())
	provider.refresh(context.Background())

	got := pool.Backends()
	if len(got) != 2 {
		t.Fatalf("expected 2 backends after refresh, got %d", len(got))
	}
	if got[0].Name != "b1" || got[1].Name != "b2" {
		t.Errorf("unexpected backend names: %v", got)
	}
}

func TestConfigProvider_ReusesExistingBackendForUnchangedSpec(t *testing.T) {
	var mu sync.Mutex
	weight := 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		w_ := weight
		mu.Unlock()
		fmt.Fprintf(w, `{"backends":[{"name":"b1","url":"http://localhost:9001","weight":%d}]}`, w_)
	}))
	defer srv.Close()

	pool := NewPool(nil)
	provider := NewConfigProvider(pool, srv.URL, time.Hour, testLogger())
	provider.refresh(context.Background())

	first := pool.Backends()[0]
	first.SetAlive(false) // simulate health state accumulated since the first fetch
	first.IncConnections()

	provider.refresh(context.Background())

	second := pool.Backends()[0]
	if second != first {
		t.Error("expected refresh to reuse the same *Backend pointer when name/url/weight are unchanged")
	}
	if second.IsAlive() {
		t.Error("expected health state to survive a refresh that reuses the backend")
	}
	if second.ActiveConnections() != 1 {
		t.Error("expected connection count to survive a refresh that reuses the backend")
	}

	mu.Lock()
	weight = 5
	mu.Unlock()
	provider.refresh(context.Background())

	third := pool.Backends()[0]
	if third == first {
		t.Error("expected a new *Backend when weight changes")
	}
}

func TestConfigProvider_HandlesFetchFailureGracefully(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b1, _ := New("b1", "http://localhost:9001", 1)
	pool := NewPool([]*Backend{b1})
	provider := NewConfigProvider(pool, srv.URL, time.Hour, testLogger())
	provider.refresh(context.Background())

	if len(pool.Backends()) != 1 {
		t.Error("a failed refresh must not clear the existing pool")
	}
}
