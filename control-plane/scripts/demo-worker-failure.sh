#!/usr/bin/env bash
# Demonstrates failure recovery: submits a workload, hard-kills a worker
# container while jobs are running on it, and shows the control plane detect
# the missed heartbeat, mark the worker unhealthy, and reschedule its jobs.
# Run ./run-local-cluster.sh first.
set -euo pipefail

cd "$(dirname "$0")/.."

INFRACTL_BIN="$(mktemp -d)/infractl"
go build -o "$INFRACTL_BIN" ./cmd/infractl
export INFRACTL_SERVER="${INFRACTL_SERVER:-http://localhost:7070}"

echo "1. Submitting a workload with 12 long-running job instances..."
TMP_WORKLOAD="$(mktemp)"
cat > "$TMP_WORKLOAD" <<'EOF'
name: failure-recovery-demo
type: batch
command: "sleep"
args: ["8"]
replicas: 12
max_retries: 3
restart_policy: on_failure
resources:
  cpu: 0.1
  memory_mb: 32
EOF
"$INFRACTL_BIN" workload submit "$TMP_WORKLOAD"
WORKLOAD_ID=$("$INFRACTL_BIN" workload list | awk 'NR==2{print $1}')

echo
echo "2. Waiting for jobs to be scheduled across workers..."
sleep 3
"$INFRACTL_BIN" workload status "$WORKLOAD_ID" | tail -n +6

echo
echo "3. Hard-killing worker-2 (simulates a crashed node, not a graceful stop)..."
( cd .. && docker compose kill worker-2 )
KILL_TIME=$(date +%s)

echo
echo "4. Waiting for the control plane's heartbeat timeout to fire (default 15s,
   plus reconcile interval) -- this will take ~17-20s..."
while true; do
  STATUS=$("$INFRACTL_BIN" worker list | grep "worker-2:9101" | awk '{print $3}' || true)
  NOW=$(date +%s)
  ELAPSED=$((NOW - KILL_TIME))
  echo "  [+${ELAPSED}s] worker-2 status: ${STATUS:-unknown}"
  if [ "$STATUS" = "UNHEALTHY" ]; then
    break
  fi
  if [ "$ELAPSED" -gt 30 ]; then
    echo "  timed out waiting for worker-2 to be marked unhealthy" >&2
    break
  fi
  sleep 2
done

echo
echo "5. Worker fleet after detection:"
"$INFRACTL_BIN" worker list

echo
echo "6. Workload status -- jobs that were running on worker-2 should now be
   PENDING/SCHEDULED again (or RETRYING with backoff), being picked up by
   the remaining healthy workers:"
"$INFRACTL_BIN" workload status "$WORKLOAD_ID" | tail -n +6

echo
echo "Done. Restart worker-2 with: docker compose start worker-2"
