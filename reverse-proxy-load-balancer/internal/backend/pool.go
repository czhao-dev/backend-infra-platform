package backend

import "sync/atomic"

// Pool is a set of backends shared by the load balancer and the health
// checker. The backend slice is held behind an atomic pointer so it can be
// swapped wholesale (e.g. by a dynamic-discovery ConfigProvider refreshing
// from the control plane) without a lock on the hot read path: Backends()
// and Healthy() are a single atomic load. Only each Backend's own atomic
// fields mutate in place; the slice itself is always replaced, never edited.
type Pool struct {
	current atomic.Pointer[poolData]
}

type poolData struct {
	backends []*Backend
}

// NewPool creates a Pool from the given backends.
func NewPool(backends []*Backend) *Pool {
	p := &Pool{}
	p.current.Store(&poolData{backends: backends})
	return p
}

// Backends returns all backends in the pool, healthy or not.
func (p *Pool) Backends() []*Backend {
	return p.current.Load().backends
}

// Healthy returns the subset of backends currently marked alive.
func (p *Pool) Healthy() []*Backend {
	backends := p.Backends()
	healthy := make([]*Backend, 0, len(backends))
	for _, b := range backends {
		if b.IsAlive() {
			healthy = append(healthy, b)
		}
	}
	return healthy
}

// Swap atomically replaces the pool's backend set, e.g. when a
// ConfigProvider fetches an updated list from the control plane. Callers
// that want to preserve health/connection-count state across a refresh
// should reuse existing *Backend pointers for names that persist rather
// than allocating new ones -- see backend.ConfigProvider.
func (p *Pool) Swap(backends []*Backend) {
	p.current.Store(&poolData{backends: backends})
}
