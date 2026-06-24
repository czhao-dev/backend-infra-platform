#!/usr/bin/env bash
# Demonstrates health-aware proxy failover: sends traffic through the
# dynamic-discovery proxy, kills a worker, shows traffic continuing on the
# remaining healthy backends, then restarts the worker and shows it rejoin
# rotation. Run ./run-local-cluster.sh first.
#
# Note: the worker agent isn't a job-serving HTTP service (jobs run as
# subprocesses) -- its small metrics listener's /readyz endpoint stands in
# as "traffic" here, which is enough to demonstrate health-aware routing and
# failover without conflating it with a real workload's behavior.
set -euo pipefail

cd "$(dirname "$0")/.."

PROXY_URL="${PROXY_URL:-http://localhost:8081}"

send_burst() {
  local label="$1" n="${2:-10}"
  echo "  $label:"
  for i in $(seq 1 "$n"); do
    code=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/readyz" || echo "ERR")
    printf "    request %2d -> %s\n" "$i" "$code"
  done
}

echo "1. Current backends:"
curl -s "$PROXY_URL/admin/backends" | python3 -m json.tool 2>/dev/null || curl -s "$PROXY_URL/admin/backends"

echo
echo "2. Sending traffic with all workers healthy..."
send_burst "before kill"

echo
echo "3. Killing worker-1..."
( cd .. && docker compose kill worker-1 )

echo "   Waiting for the proxy's active health check to notice (a few seconds)..."
sleep 5

echo
echo "4. Backends after kill:"
curl -s "$PROXY_URL/admin/backends" | python3 -m json.tool 2>/dev/null || curl -s "$PROXY_URL/admin/backends"

echo
echo "5. Sending traffic again -- should still succeed via remaining workers:"
send_burst "after kill"

echo
echo "6. Restarting worker-1..."
( cd .. && docker compose start worker-1 )
echo "   Waiting for it to re-register, pass health checks, and for the proxy's
   discovery refresh (every 5s) to drop the dead worker's stale entry..."
sleep 12

echo
echo "7. Backends after restart:"
curl -s "$PROXY_URL/admin/backends" | python3 -m json.tool 2>/dev/null || curl -s "$PROXY_URL/admin/backends"

echo
echo "Done."
