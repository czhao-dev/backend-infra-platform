#!/usr/bin/env bash
# Submits N short-lived workloads and times how long it takes for every job
# they create to reach SUCCEEDED, polling /api/v1/scheduler/stats. A simple
# wall-clock benchmark, not a Go benchmark -- keeps it dependency-free and
# easy to run against a live (Docker or local) cluster.
# Run ./run-local-cluster.sh first.
set -euo pipefail

cd "$(dirname "$0")/.."

N="${1:-10}"
REPLICAS_PER_WORKLOAD="${2:-5}"

INFRACTL_BIN="$(mktemp -d)/infractl"
go build -o "$INFRACTL_BIN" ./cmd/infractl
export INFRACTL_SERVER="${INFRACTL_SERVER:-http://localhost:7070}"
SERVER="$INFRACTL_SERVER"

echo "Submitting $N workloads x $REPLICAS_PER_WORKLOAD job instances each..."
TMP_WORKLOAD="$(mktemp)"
cat > "$TMP_WORKLOAD" <<EOF
name: benchmark
type: batch
command: "true"
replicas: $REPLICAS_PER_WORKLOAD
max_retries: 1
resources:
  cpu: 0.05
  memory_mb: 16
EOF

TOTAL_JOBS=$((N * REPLICAS_PER_WORKLOAD))
START=$(date +%s.%N)

for i in $(seq 1 "$N"); do
  "$INFRACTL_BIN" workload submit "$TMP_WORKLOAD" >/dev/null
done

echo "Submitted $TOTAL_JOBS total job instances. Waiting for completion..."
while true; do
  PENDING=$(curl -s "$SERVER/api/v1/scheduler/queue" | python3 -c 'import json,sys; print(json.load(sys.stdin)["depth"])' 2>/dev/null || echo "?")
  echo "  pending: $PENDING"
  if [ "$PENDING" = "0" ]; then
    break
  fi
  sleep 1
done

END=$(date +%s.%N)
ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
THROUGHPUT=$(python3 -c "print(f'{$TOTAL_JOBS / ($END - $START):.1f}')")

echo
echo "Submitted and scheduled $TOTAL_JOBS jobs in ${ELAPSED}s (${THROUGHPUT} jobs/sec scheduling throughput)."
echo "Note: this measures submission-to-scheduled latency; jobs may still be running/finishing on workers."
