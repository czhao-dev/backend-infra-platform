#!/usr/bin/env bash
# Submits the example batch workload (20 job instances) to a running
# control plane and shows how the scheduler distributes them across workers.
# Run ./run-local-cluster.sh first.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT_DIR="$(pwd)"

INFRACTL_BIN="$(mktemp -d)/infractl"
go build -o "$INFRACTL_BIN" ./cmd/infractl

export INFRACTL_SERVER="${INFRACTL_SERVER:-http://localhost:7070}"

echo "Submitting examples/batch-job.yaml (20 job instances) to $INFRACTL_SERVER..."
"$INFRACTL_BIN" workload submit examples/batch-job.yaml

WORKLOAD_ID=$("$INFRACTL_BIN" workload list | awk 'NR==2{print $1}')
echo "Workload ID: $WORKLOAD_ID"

for i in 1 2 3 4 5; do
  echo
  echo "--- after ${i}s ---"
  "$INFRACTL_BIN" worker list
  echo
  "$INFRACTL_BIN" workload status "$WORKLOAD_ID" | tail -n +6
  sleep 1
done

echo
echo "Done. Re-run \`infractl workload status $WORKLOAD_ID\` to keep watching."
