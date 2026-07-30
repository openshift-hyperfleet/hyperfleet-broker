#!/usr/bin/env bash
# Brings up the full HyperFleet pipeline in kind with Postgres as the broker:
#   API (helm) -> Postgres/river (manifests) -> Sentinel (helm, broker.type=river)
#   -> adapter (helm, river-poc example task).
#
# Requirements: podman, kind, kubectl, helm. Repo checkouts as siblings of
# hyperfleet-broker (override with API_DIR / SENTINEL_DIR / ADAPTER_DIR).
# The sentinel and adapter checkouts must be on their poc/river-broker
# branches (broker fork-replace + chart river support).
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-hyperfleet}"
NAMESPACE="${NAMESPACE:-hyperfleet}"
IMAGE_TAG="${IMAGE_TAG:-river-poc}"
PF_PORT="${PF_PORT:-18000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
SIBLINGS="$(dirname "${BROKER_ROOT}")"
API_DIR="${API_DIR:-${SIBLINGS}/hyperfleet-api}"
SENTINEL_DIR="${SENTINEL_DIR:-${SIBLINGS}/hyperfleet-sentinel}"
ADAPTER_DIR="${ADAPTER_DIR:-${SIBLINGS}/hyperfleet-adapter}"
export KIND_EXPERIMENTAL_PROVIDER=podman

RIVER_DSN="postgres://broker:broker@river-postgres.${NAMESPACE}.svc.cluster.local:5432/broker?sslmode=disable"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "Checking prerequisites"
for tool in podman kind kubectl helm; do
  command -v "$tool" >/dev/null || { echo "missing: $tool"; exit 1; }
done
for dir in "$API_DIR" "$SENTINEL_DIR" "$ADAPTER_DIR"; do
  [[ -d "$dir" ]] || { echo "missing repo checkout: $dir"; exit 1; }
done
systemctl --user is-active --quiet podman.socket || systemctl --user start podman.socket

step "Creating kind cluster '${CLUSTER_NAME}' (skipped if it exists)"
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --name "${CLUSTER_NAME}" --wait 120s
fi
kubectl config use-context "kind-${CLUSTER_NAME}"

step "Building hyperfleet-api image"
make -C "${API_DIR}" image IMAGE_REGISTRY=localhost IMAGE_NAME=hyperfleet-api IMAGE_TAG="${IMAGE_TAG}"

step "Building hyperfleet-sentinel image"
podman build -t "localhost/hyperfleet-sentinel:${IMAGE_TAG}" "${SENTINEL_DIR}"

step "Building hyperfleet-adapter image"
podman build -t "localhost/hyperfleet-adapter:${IMAGE_TAG}" "${ADAPTER_DIR}"

step "Loading images into kind"
ARCHIVE="$(mktemp --suffix=.tar)"
trap 'rm -f "${ARCHIVE}"' EXIT
podman save -m \
  "localhost/hyperfleet-api:${IMAGE_TAG}" \
  "localhost/hyperfleet-sentinel:${IMAGE_TAG}" \
  "localhost/hyperfleet-adapter:${IMAGE_TAG}" \
  -o "${ARCHIVE}"
kind load image-archive "${ARCHIVE}" --name "${CLUSTER_NAME}"

step "Installing hyperfleet-api chart"
# Cluster's required adapter is overridden to the demo adapter so the
# aggregated Reconciled condition can reach True with one adapter deployed.
ENTITIES_JSON='[
  {"kind":"Cluster","plural":"clusters","spec_schema_name":"ClusterSpec",
   "required_adapters":["river-poc-kubernetes"],
   "name_min_len":3,"name_max_len":53,"require_spec_schema":true},
  {"kind":"NodePool","plural":"nodepools","parent_kind":"Cluster","on_parent_delete":"cascade",
   "spec_schema_name":"NodePoolSpec","required_adapters":["river-poc-kubernetes"],
   "name_min_len":3,"name_max_len":15,"require_spec_schema":true},
  {"kind":"Channel","plural":"channels","spec_schema_name":"ChannelSpec"},
  {"kind":"Version","plural":"versions","parent_kind":"Channel","on_parent_delete":"restrict","spec_schema_name":"VersionSpec"},
  {"kind":"WifConfig","plural":"wifconfigs","spec_schema_name":"WifConfigSpec"}
]'
helm upgrade --install hyperfleet-api "${API_DIR}/charts" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set image.registry=localhost \
  --set image.repository=hyperfleet-api \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=Never \
  --set-json "config.entities=${ENTITIES_JSON}"
kubectl -n "${NAMESPACE}" rollout status deploy/hyperfleet-api --timeout=180s

step "Deploying Postgres for the river broker (single replica)"
kubectl -n "${NAMESPACE}" apply -f "${SCRIPT_DIR}/../manifests/postgres.yaml"
kubectl -n "${NAMESPACE}" rollout status deploy/river-postgres --timeout=120s

step "Installing sentinel chart (broker.type=river)"
helm upgrade --install sentinel "${SENTINEL_DIR}/charts" \
  --namespace "${NAMESPACE}" \
  --set image.registry=localhost \
  --set image.repository=hyperfleet-sentinel \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=Never \
  --set broker.type=river \
  --set-string "broker.river.dsn=${RIVER_DSN}" \
  --set broker.river.createSchemaIfMissing=true
kubectl -n "${NAMESPACE}" rollout status deploy/sentinel-hyperfleet-sentinel --timeout=180s

step "Installing adapter chart (river-poc example)"
(cd "${ADAPTER_DIR}/charts" && helm upgrade --install adapter . \
  --namespace "${NAMESPACE}" \
  -f examples/river-poc/values.yaml)
kubectl -n "${NAMESPACE}" rollout status deploy/adapter-hyperfleet-adapter --timeout=180s || \
  kubectl -n "${NAMESPACE}" get deploy

step "Starting port-forward localhost:${PF_PORT} -> hyperfleet-api:8000"
PF_PID_FILE="${SCRIPT_DIR}/.port-forward.pid"
if [[ -f "${PF_PID_FILE}" ]] && kill -0 "$(cat "${PF_PID_FILE}")" 2>/dev/null; then
  echo "port-forward already running (pid $(cat "${PF_PID_FILE}"))"
else
  nohup kubectl -n "${NAMESPACE}" port-forward svc/hyperfleet-api "${PF_PORT}:8000" \
    >/dev/null 2>&1 &
  echo $! > "${PF_PID_FILE}"
  sleep 2
fi

step "Sanity check"
curl -sf "http://localhost:${PF_PORT}/api/hyperfleet/v1/clusters" | head -c 200; echo
kubectl -n "${NAMESPACE}" get pods

cat <<EOF

Pipeline is up. Next:
  ./10-lifecycle-test.sh    # create a cluster via the API and watch it flow through Postgres
  ./20-load-test.sh         # push load through the river backend and report latency
  kubectl -n ${NAMESPACE} logs deploy/sentinel-hyperfleet-sentinel -f    # sentinel decisions
  kubectl -n ${NAMESPACE} exec deploy/river-postgres -- psql -U broker -c 'TABLE broker_subscriptions'
  ./99-teardown.sh
EOF
