package backend

import (
	"sync"
	"testing"
)

func TestPool_Healthy(t *testing.T) {
	b1, _ := New("b1", "http://localhost:9001", 1)
	b2, _ := New("b2", "http://localhost:9002", 1)
	b2.SetAlive(false)

	pool := NewPool([]*Backend{b1, b2})

	healthy := pool.Healthy()
	if len(healthy) != 1 || healthy[0].Name != "b1" {
		t.Errorf("expected only b1 to be healthy, got %v", healthy)
	}

	if len(pool.Backends()) != 2 {
		t.Errorf("expected Backends() to return all backends regardless of health")
	}
}

func TestBackend_ConnectionTracking(t *testing.T) {
	b, _ := New("b1", "http://localhost:9001", 1)

	b.IncConnections()
	b.IncConnections()
	if got := b.ActiveConnections(); got != 2 {
		t.Errorf("expected 2 active connections, got %d", got)
	}

	b.DecConnections()
	if got := b.ActiveConnections(); got != 1 {
		t.Errorf("expected 1 active connection, got %d", got)
	}
}

func TestBackend_DefaultWeight(t *testing.T) {
	b, err := New("b1", "http://localhost:9001", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Weight != 1 {
		t.Errorf("expected default weight of 1, got %d", b.Weight)
	}
}

func TestBackend_InvalidURL(t *testing.T) {
	if _, err := New("b1", "http://[::1]:namedport", 1); err == nil {
		t.Error("expected error for invalid backend URL")
	}
}

func TestPool_SwapReplacesBackends(t *testing.T) {
	b1, _ := New("b1", "http://localhost:9001", 1)
	pool := NewPool([]*Backend{b1})

	if len(pool.Backends()) != 1 {
		t.Fatalf("expected 1 backend before swap, got %d", len(pool.Backends()))
	}

	b2, _ := New("b2", "http://localhost:9002", 1)
	b3, _ := New("b3", "http://localhost:9003", 1)
	pool.Swap([]*Backend{b2, b3})

	got := pool.Backends()
	if len(got) != 2 || got[0].Name != "b2" || got[1].Name != "b3" {
		t.Errorf("expected swapped backends [b2 b3], got %v", got)
	}
}

func TestPool_SwapIsRaceFree(t *testing.T) {
	b1, _ := New("b1", "http://localhost:9001", 1)
	pool := NewPool([]*Backend{b1})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			b, _ := New("bx", "http://localhost:9999", 1)
			pool.Swap([]*Backend{b})
		}()
		go func() {
			defer wg.Done()
			_ = pool.Healthy()
			_ = pool.Backends()
		}()
	}
	wg.Wait()
}
