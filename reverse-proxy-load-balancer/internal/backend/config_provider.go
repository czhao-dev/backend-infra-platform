package backend

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// backendSpec mirrors the control plane's model.BackendSpec DTO -- kept as
// an unexported local type rather than an import so this module has no
// dependency on the control-plane module, preserving its standalone build.
type backendSpec struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Weight int    `json:"weight"`
}

type backendsResponse struct {
	Backends []backendSpec `json:"backends"`
}

// ConfigProvider periodically polls a control plane's backend-discovery
// endpoint (e.g. GET /api/v1/proxy/backends) and swaps Pool's backend set to
// match. This is the dynamic-discovery alternative to the proxy's normal
// static-YAML backend list -- see cmd/proxy/main.go, which runs one or the
// other depending on config.Discovery.Enabled.
type ConfigProvider struct {
	pool     *Pool
	url      string
	interval time.Duration
	client   *http.Client
	logger   *slog.Logger

	known map[string]*Backend // name -> existing Backend, reused across refreshes to preserve health/conn state
}

func NewConfigProvider(pool *Pool, url string, interval time.Duration, logger *slog.Logger) *ConfigProvider {
	return &ConfigProvider{
		pool:     pool,
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
		logger:   logger,
		known:    make(map[string]*Backend),
	}
}

// Run blocks, refreshing the pool on a fixed interval until ctx is
// cancelled. An initial fetch runs immediately so the pool is populated
// before the first request arrives.
func (c *ConfigProvider) Run(ctx context.Context) {
	c.refresh(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *ConfigProvider) refresh(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		c.logger.Error("config provider: build request", "error", err)
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warn("config provider: fetch failed", "url", c.url, "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logger.Warn("config provider: unexpected status", "url", c.url, "status", resp.Status)
		return
	}

	var parsed backendsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		c.logger.Error("config provider: decode response", "error", err)
		return
	}

	next := make(map[string]*Backend, len(parsed.Backends))
	backends := make([]*Backend, 0, len(parsed.Backends))
	for _, spec := range parsed.Backends {
		if existing, ok := c.known[spec.Name]; ok && existing.URL.String() == spec.URL && existing.Weight == spec.Weight {
			// Reuse the existing *Backend so its health state and
			// connection/request counters survive this refresh.
			next[spec.Name] = existing
			backends = append(backends, existing)
			continue
		}
		b, err := New(spec.Name, spec.URL, spec.Weight)
		if err != nil {
			c.logger.Error("config provider: invalid backend", "name", spec.Name, "url", spec.URL, "error", err)
			continue
		}
		next[spec.Name] = b
		backends = append(backends, b)
	}

	c.known = next
	c.pool.Swap(backends)
	c.logger.Info("config provider: refreshed backends", "count", len(backends))
}
