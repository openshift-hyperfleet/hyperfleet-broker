#!/usr/bin/env bash
# Pushes synthetic load through the in-cluster river broker and reports
# end-to-end latency. Uses its own topic and subscription IDs so the demo
# adapter's queue is untouched.
set -euo pipefail

NAMESPACE="${NAMESPACE:-hyperfleet}"
PG_PORT="${PG_PORT:-15432}"
RATE="${RATE:-200}"
DURATION="${DURATION:-30s}"
SUBS="${SUBS:-3}"
PARALLELISM="${PARALLELISM:-8}"
FETCH_COOLDOWN="${FETCH_COOLDOWN:-10ms}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
DSN="postgres://broker:broker@localhost:${PG_PORT}/broker?sslmode=disable"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pgshow() { kubectl -n "${NAMESPACE}" exec deploy/river-postgres -- psql -U broker -c "$1"; }

step "Port-forwarding river-postgres to localhost:${PG_PORT}"
kubectl -n "${NAMESPACE}" port-forward svc/river-postgres "${PG_PORT}:5432" >/dev/null 2>&1 &
PF_PID=$!
trap 'kill ${PF_PID} 2>/dev/null || true' EXIT
sleep 2

step "Table sizes before"
pgshow "SELECT pg_size_pretty(pg_total_relation_size('river_job')) AS river_job_size"

step "Running loadgen: ${RATE}/sec for ${DURATION}, ${SUBS}-way fan-out"
(cd "${BROKER_ROOT}" && go run ./poc/river-pipeline/loadgen \
  -dsn "${DSN}" \
  -topic load-topic \
  -rate "${RATE}" \
  -duration "${DURATION}" \
  -subs "${SUBS}" \
  -parallelism "${PARALLELISM}" \
  -fetch-cooldown "${FETCH_COOLDOWN}")

step "Job state and table sizes after"
pgshow "SELECT queue, state, count(*) FROM river_job GROUP BY 1, 2 ORDER BY 1, 2"
pgshow "SELECT pg_size_pretty(pg_total_relation_size('river_job')) AS river_job_size"

cat <<EOF

Note: completed jobs are pruned by River's maintenance after the completed
job retention period (default 24h); table size at queue-idle steady state
is governed by that retention, not by the burst above.
EOF
