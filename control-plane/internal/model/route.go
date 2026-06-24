package model

import "time"

// LoadBalanceStrategy names one of the reverse proxy's balancing strategies.
type LoadBalanceStrategy string

const (
	StrategyRoundRobin LoadBalanceStrategy = "round_robin"
	StrategyLeastConn  LoadBalanceStrategy = "least_conn"
	StrategyWeighted   LoadBalanceStrategy = "weighted_round_robin"
)

// RetryPolicy is the proxy's per-route retry behavior.
type RetryPolicy struct {
	MaxAttempts     int `json:"max_attempts"`
	PerTryTimeoutMS int `json:"per_try_timeout_ms"`
}

// HealthCheckConfig is the proxy's per-route active health-check behavior.
type HealthCheckConfig struct {
	Path            string `json:"path"`
	IntervalSeconds int    `json:"interval_seconds"`
}

// BackendSpec is a flat, JSON-serializable backend descriptor returned to the
// reverse proxy via GET /api/v1/proxy/backends. It is intentionally distinct
// from reverse-proxy-load-balancer's own backend.Backend runtime type (which
// tracks atomic health/connection-count state) — this is just a DTO.
type BackendSpec struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Weight int    `json:"weight"`
}

// Route describes how the reverse proxy should route traffic for a path
// prefix. Routes are config data, not a workflow entity, so they have no
// state machine — just create/update/delete.
type Route struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	PathPrefix  string              `json:"path_prefix"`
	Strategy    LoadBalanceStrategy `json:"strategy"`
	Backends    []BackendSpec       `json:"backends"`
	RetryPolicy RetryPolicy         `json:"retry_policy"`
	HealthCheck HealthCheckConfig   `json:"health_check"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

// Clone returns a deep copy of the Route.
func (r Route) Clone() Route {
	c := r
	if r.Backends != nil {
		c.Backends = append([]BackendSpec(nil), r.Backends...)
	}
	return c
}
