#!/usr/bin/env bash
# Tears down the river pipeline POC environment.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-hyperfleet}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KIND_EXPERIMENTAL_PROVIDER=podman

PF_PID_FILE="${SCRIPT_DIR}/.port-forward.pid"
if [[ -f "${PF_PID_FILE}" ]]; then
  kill "$(cat "${PF_PID_FILE}")" 2>/dev/null || true
  rm -f "${PF_PID_FILE}"
  echo "stopped port-forward"
fi

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

if [[ "${1:-}" == "--images" ]]; then
  podman rmi -f \
    localhost/hyperfleet-api:river-poc \
    localhost/hyperfleet-sentinel:river-poc \
    localhost/hyperfleet-adapter:river-poc 2>/dev/null || true
  echo "removed local images"
fi

echo "done"
