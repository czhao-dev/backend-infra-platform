#!/usr/bin/env bash
# Builds and starts the control-plane stack (control plane, 3 workers, the
# dynamic-discovery proxy, Prometheus, and Grafana) via the root
# docker-compose.yml. Does not start the standalone orchestrator/proxy stack
# (that's still available separately -- see the root README).
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "Building and starting the control-plane stack..."
docker compose up --build -d control-plane worker-1 worker-2 worker-3 dynamic-proxy prometheus grafana

echo "Waiting for control plane to become ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:7070/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -sf http://localhost:7070/healthz >/dev/null || { echo "control plane did not become ready" >&2; exit 1; }

echo "Waiting for dynamic proxy to become ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo
echo "Cluster is up:"
echo "  Control plane API:   http://localhost:7070"
echo "  Dynamic proxy:        http://localhost:8081"
echo "  Prometheus:           http://localhost:9090"
echo "  Grafana (admin/admin): http://localhost:3000"
echo
echo "Next: ./control-plane/scripts/submit-demo-jobs.sh"
