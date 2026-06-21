# Go Reverse Proxy / Load Balancer

A lightweight reverse proxy and HTTP load balancer written in Go. This project implements request forwarding, backend health checks, multiple load-balancing strategies, retry/failover behavior, structured logging, metrics, and graceful shutdown.

The goal is to demonstrate backend infrastructure concepts commonly used in API gateways, service meshes, edge proxies, and internal traffic routers.

## Overview

`go-load-balancer` is a from-scratch reverse proxy that accepts incoming HTTP requests and forwards them to a pool of backend servers.

It supports:

* reverse proxy request forwarding
* multiple backend servers
* round-robin load balancing
* least-connections load balancing
* weighted round-robin, optional
* active health checks
* passive failure detection
* retry and failover
* per-backend connection tracking
* request timeout handling
* structured access logs
* Prometheus-style metrics
* hot-reloadable configuration, optional
* graceful shutdown

This project is intended as a backend systems project for understanding how production load balancers route traffic, detect unhealthy backends, and maintain high availability.

## Motivation

Reverse proxies and load balancers are core infrastructure components in distributed systems. They sit between clients and backend services and are responsible for routing, availability, observability, and failure handling.

This project explores important backend engineering concepts:

* HTTP request forwarding
* service discovery and backend pools
* health checking
* load-balancing algorithms
* retry and timeout policies
* connection management
* graceful shutdown
* observability and metrics
* configuration-driven infrastructure

The goal is not to replace mature systems such as NGINX, HAProxy, Envoy, or Traefik. Instead, this project implements the core ideas in a small, readable Go codebase.

## Features

### Reverse Proxy

* [x] Forward HTTP requests to backend servers
* [x] Preserve request path and query parameters
* [x] Forward selected headers
* [x] Add proxy headers such as `X-Forwarded-For`
* [x] Stream response bodies back to clients
* [x] Handle backend errors with proper HTTP responses

### Load Balancing

* [x] Round-robin selection
* [x] Least-connections selection
* [ ] Weighted round-robin
* [ ] Random selection
* [ ] Consistent hashing, optional

### Health Checking

* [x] Active health checks
* [x] Mark unhealthy backends as unavailable
* [x] Restore recovered backends automatically
* [x] Configurable health-check interval
* [x] Configurable health-check path
* [ ] Passive health checks based on request failures

### Reliability

* [x] Backend retry and failover
* [x] Request timeout handling
* [x] Graceful shutdown
* [x] Connection tracking
* [ ] Circuit breaker, optional
* [ ] Rate limiting, optional

### Observability

* [x] Structured access logs
* [x] Backend health status logs
* [x] Request latency tracking
* [x] Per-backend request counters
* [x] Metrics endpoint
* [ ] Prometheus integration
* [ ] Tracing, optional

### Developer Experience

* [x] YAML-based configuration
* [x] Docker Compose demo environment
* [x] Unit tests
* [x] Integration tests
* [x] Benchmark tests
* [x] Example backend services

Update this section as implementation progresses.

## Architecture

```text id="6zfyqm"
                  +------------------+
                  |     Client       |
                  +--------+---------+
                           |
                           v
                  +------------------+
                  | Reverse Proxy    |
                  | Load Balancer    |
                  +--------+---------+
                           |
              +------------+------------+
              |                         |
              v                         v
     +------------------+      +------------------+
     | Backend Server 1 |      | Backend Server 2 |
     +------------------+      +------------------+
              |
              v
     +------------------+
     | Backend Server N |
     +------------------+
```

Internal components:

```text id="0v07pk"
+---------------------+
| HTTP Server         |
|---------------------|
| accepts requests    |
| handles shutdown    |
+----------+----------+
           |
           v
+---------------------+
| Proxy Handler       |
|---------------------|
| rewrites request    |
| forwards request    |
| streams response    |
+----------+----------+
           |
           v
+---------------------+
| Load Balancer       |
|---------------------|
| selects backend     |
| tracks connections  |
| applies strategy    |
+----------+----------+
           |
           v
+---------------------+
| Backend Pool        |
|---------------------|
| backend state       |
| health status       |
| failure counters    |
+----------+----------+
           |
           v
+---------------------+
| Health Checker      |
|---------------------|
| probes backends     |
| updates status      |
+---------------------+
```

## Load-Balancing Strategies

### Round Robin

Round robin distributes requests evenly across healthy backends.

```text id="md9gjb"
request 1 -> backend 1
request 2 -> backend 2
request 3 -> backend 3
request 4 -> backend 1
```

This strategy is simple and works well when all backends have similar capacity.

### Least Connections

Least-connections routing sends the next request to the healthy backend with the fewest active requests.

This strategy is useful when requests have variable latency or when some backends are temporarily busier than others.

### Weighted Round Robin

Weighted round robin allows stronger backends to receive more traffic.

Example:

```text id="151pmo"
backend-a weight 3
backend-b weight 1

backend-a receives about 75% of requests
backend-b receives about 25% of requests
```

### Consistent Hashing, Optional

Consistent hashing can route requests with the same key to the same backend.

Possible hash keys:

* client IP
* request path
* user ID header
* session ID cookie

This is useful for cache locality and session affinity.

## Health Checks

The proxy periodically checks each backend using a configurable health endpoint.

Example:

```text id="yod5b8"
GET /health
```

A backend is considered healthy if it responds with a success status before the configured timeout.

Health-check behavior:

```text id="519zid"
healthy backend   -> eligible for traffic
unhealthy backend -> skipped by load balancer
recovered backend -> automatically re-added
```

Example log:

```text id="qf7i3u"
backend=http://localhost:9001 status=healthy latency=3ms
backend=http://localhost:9002 status=unhealthy error="connection refused"
```

## Retry and Failover

If a selected backend fails, the proxy can retry the request against another healthy backend.

Example behavior:

```text id="jz91el"
client request
  -> backend-1 fails
  -> retry backend-2
  -> response returned to client
```

Retries should be limited to avoid amplifying traffic during outages.

Recommended policy:

```text id="56mpc7"
max_retries: 2
per_request_timeout: 2s
backend_timeout: 1s
```

For safety, retry behavior should be conservative for non-idempotent methods such as `POST`, `PUT`, and `PATCH`.

## Configuration

Example `config.yaml`:

```yaml id="mtzbyu"
server:
  listen_addr: ":8080"
  read_timeout: "5s"
  write_timeout: "10s"
  shutdown_timeout: "5s"

load_balancer:
  strategy: "round_robin"
  max_retries: 2

health_check:
  enabled: true
  path: "/health"
  interval: "2s"
  timeout: "500ms"
  unhealthy_threshold: 2
  healthy_threshold: 2

backends:
  - name: "backend-1"
    url: "http://localhost:9001"
    weight: 1

  - name: "backend-2"
    url: "http://localhost:9002"
    weight: 1

  - name: "backend-3"
    url: "http://localhost:9003"
    weight: 1

logging:
  level: "info"
  format: "json"

metrics:
  enabled: true
  path: "/metrics"
```

## API Endpoints

| Endpoint          | Description                       |
| ----------------- | --------------------------------- |
| `/`               | Proxied request path              |
| `/healthz`        | Health check for the proxy itself |
| `/metrics`        | Runtime and request metrics       |
| `/admin/backends` | Backend status, optional          |
| `/admin/reload`   | Reload configuration, optional    |

## Metrics

The proxy exposes metrics for observability.

Example metrics:

```text id="zk1u18"
proxy_requests_total
proxy_request_duration_seconds
proxy_backend_requests_total
proxy_backend_errors_total
proxy_backend_active_connections
proxy_backend_health_status
proxy_retries_total
proxy_up
```

Example `/metrics` output:

```text id="2gy6s1"
proxy_requests_total 1024
proxy_backend_requests_total{backend="backend-1"} 341
proxy_backend_requests_total{backend="backend-2"} 342
proxy_backend_requests_total{backend="backend-3"} 341
proxy_backend_errors_total{backend="backend-2"} 3
proxy_backend_active_connections{backend="backend-1"} 7
proxy_backend_health_status{backend="backend-1"} 1
```

## Project Structure

Suggested layout:

```text id="v29n3y"
go-load-balancer/
├── README.md
├── go.mod
├── go.sum
├── Dockerfile
├── docker-compose.yml
├── config.yaml
├── cmd/
│   ├── proxy/
│   │   └── main.go
│   └── backend/
│       └── main.go
├── internal/
│   ├── config/
│   │   └── config.go
│   ├── proxy/
│   │   ├── handler.go
│   │   ├── director.go
│   │   └── transport.go
│   ├── balancer/
│   │   ├── balancer.go
│   │   ├── round_robin.go
│   │   ├── least_conn.go
│   │   └── weighted.go
│   ├── backend/
│   │   ├── backend.go
│   │   └── pool.go
│   ├── health/
│   │   └── checker.go
│   ├── metrics/
│   │   └── metrics.go
│   ├── logging/
│   │   └── logger.go
│   └── admin/
│       └── handler.go
├── tests/
│   ├── integration/
│   └── load/
└── scripts/
    ├── run-demo.sh
    └── load-test.sh
```

## Quick Start

Clone the repository:

```bash id="d3xbrg"
git clone https://github.com/czhao-dev/go-load-balancer.git
cd go-load-balancer
```

Run tests:

```bash id="hcj7gx"
go test ./...
```

Run the demo with Docker Compose:

```bash id="695ttj"
docker compose up --build
```

This starts:

```text id="6bx3bt"
proxy      -> localhost:8080
backend-1  -> localhost:9001
backend-2  -> localhost:9002
backend-3  -> localhost:9003
```

Send requests:

```bash id="9tl3y6"
curl http://localhost:8080/api/hello
curl http://localhost:8080/api/hello
curl http://localhost:8080/api/hello
```

Check backend status:

```bash id="ey13p8"
curl http://localhost:8080/admin/backends
```

Check metrics:

```bash id="vjzf6n"
curl http://localhost:8080/metrics
```

## Example Backend Server

The repository includes a small demo backend server.

Example response:

```json id="b40hqn"
{
  "backend": "backend-1",
  "path": "/api/hello",
  "message": "hello from backend-1"
}
```

When round-robin balancing is enabled, repeated requests should rotate through available backends.

## Load Testing

Use `hey`, `wrk`, or `ab` to test the proxy.

Example using `hey`:

```bash id="elrhik"
hey -n 10000 -c 100 http://localhost:8080/api/hello
```

Example output to include in the README after testing:

```text id="m9uxjj"
Total requests:        10000
Concurrency:           100
Average latency:       8.4 ms
p95 latency:           18.7 ms
p99 latency:           31.2 ms
Requests/sec:          11800
```

## Testing Strategy

### Unit Tests

Unit tests should cover:

* round-robin backend selection
* least-connections backend selection
* weighted backend selection
* unhealthy backends are skipped
* retry limit is respected
* request headers are forwarded correctly
* backend connection counters are updated correctly
* configuration parsing

### Integration Tests

Integration tests should cover:

* proxy forwards requests to healthy backends
* traffic is distributed across multiple backends
* failed backend is removed from rotation
* recovered backend is added back into rotation
* request timeout returns proper error
* graceful shutdown stops accepting new requests
* metrics endpoint exposes expected counters

### Load Tests

Load tests should measure:

* throughput
* average latency
* p95/p99 latency
* retry behavior under backend failure
* health-check recovery time
* behavior under uneven backend latency

## Design Notes

### Why Go?

Go is a strong fit for this project because it provides:

* efficient HTTP server and client libraries
* lightweight goroutines
* simple concurrency primitives
* good networking support
* straightforward deployment
* strong testing and benchmarking tools

### Why active health checks?

Active health checks let the proxy proactively remove bad backends before routing real client traffic to them.

### Why least-connections?

Round robin works well when requests are uniform. Least-connections is better when requests have different processing times because it avoids overloading already busy backends.

### Why graceful shutdown?

A production proxy should stop accepting new requests while allowing in-flight requests to complete before exiting.

## Failure Handling

The proxy handles several failure cases:

| Failure                    | Behavior                               |
| -------------------------- | -------------------------------------- |
| Backend connection refused | Retry another healthy backend          |
| Backend timeout            | Return `504 Gateway Timeout` or retry  |
| No healthy backends        | Return `503 Service Unavailable`       |
| Invalid backend response   | Return `502 Bad Gateway`               |
| Proxy shutting down        | Stop accepting new requests            |
| Health check failure       | Mark backend unhealthy after threshold |

## Security Considerations

This project is primarily focused on reverse proxy and load-balancing mechanics. For production-style hardening, consider adding:

* TLS termination
* request size limits
* rate limiting
* IP allow/deny lists
* header sanitization
* authentication for admin endpoints
* protection against slowloris-style clients
* access log redaction
* secure default timeouts

## Roadmap

### Phase 1: Basic Reverse Proxy

* [ ] Parse backend config
* [ ] Forward requests to backend
* [ ] Preserve path and query parameters
* [ ] Return backend response to client

### Phase 2: Load Balancing

* [ ] Implement round robin
* [ ] Implement least connections
* [ ] Add backend connection counters
* [ ] Skip unhealthy backends

### Phase 3: Health Checks and Failover

* [ ] Add active health checks
* [ ] Mark backends healthy/unhealthy
* [ ] Add retry policy
* [ ] Return 503 when no healthy backend exists

### Phase 4: Observability

* [ ] Add structured logs
* [ ] Add metrics endpoint
* [ ] Track latency and status codes
* [ ] Add backend-level metrics

### Phase 5: Production-Style Features

* [ ] Graceful shutdown
* [ ] Hot configuration reload
* [ ] Weighted round robin
* [ ] Circuit breaker
* [ ] Rate limiting
* [ ] TLS support
* [ ] Admin API

## What This Project Demonstrates

This project demonstrates backend infrastructure skills:

* HTTP reverse proxy design
* load-balancing algorithms
* health-check design
* retry and failover behavior
* Go concurrency
* backend pool management
* timeout handling
* structured logging
* metrics and observability
* Docker-based integration testing
* distributed-systems failure handling

## Non-Goals

This project is not intended to replace production-grade systems such as NGINX, HAProxy, Envoy, or Traefik.

Non-goals:

* full HTTP/2 or HTTP/3 proxy support
* full service mesh functionality
* advanced TLS certificate management
* production-grade security hardening
* Kubernetes ingress controller support
* dynamic service discovery in the first version

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
