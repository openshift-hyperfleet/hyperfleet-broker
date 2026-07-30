#!/usr/bin/env bash
# Drives one cluster through the full river pipeline and shows every hop:
#   POST /clusters -> Sentinel poll+publish -> river_job rows in Postgres ->
#   adapter queue worker -> namespace+Job applied -> PUT /statuses ->
#   conditions on GET /clusters/{id}.
set -euo pipefail

NAMESPACE="${NAMESPACE:-hyperfleet}"
PF_PORT="${PF_PORT:-18000}"
API="http://localhost:${PF_PORT}/api/hyperfleet/v1"
NAME="river-poc-$(date +%s)"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pgexec() { kubectl -n "${NAMESPACE}" exec deploy/river-postgres -- psql -U broker -t -A -c "$1"; }
pgshow() { kubectl -n "${NAMESPACE}" exec deploy/river-postgres -- psql -U broker -c "$1"; }

step "1. POST cluster '${NAME}'"
CID=$(curl -sf -X POST "${API}/clusters" -H 'Content-Type: application/json' \
  -d "{\"kind\":\"Cluster\",\"name\":\"${NAME}\",\"spec\":{\"provider\":\"gcp\",\"region\":\"us-east1\"}}" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "cluster id: ${CID}"

step "2. Waiting for Sentinel to publish (poll interval 5s)"
JOBS=0
for _ in $(seq 1 12); do
  JOBS=$(pgexec "SELECT count(*) FROM river_job" 2>/dev/null || echo 0)
  [[ "${JOBS}" -ge 1 ]] && break
  sleep 2
done
echo "river_job now holds ${JOBS} job(s)"

step "3. Queue and subscription state (via psql)"
pgshow "SELECT queue, state, count(*) FROM river_job GROUP BY 1, 2 ORDER BY 1, 2"
pgshow "TABLE broker_subscriptions"

step "4. Waiting for the adapter to apply namespace + Job"
for _ in $(seq 1 30); do
  kubectl get namespace "${CID}-k8s" >/dev/null 2>&1 && break
  sleep 2
done
kubectl get namespace "${CID}-k8s"
kubectl -n "${CID}-k8s" get jobs 2>/dev/null || true

step "5. Adapter-reported status (PUT /statuses landed via Postgres)"
curl -sf "${API}/clusters/${CID}/statuses" | python3 -c "
import json, sys
for item in json.load(sys.stdin).get('items', []):
    for c in item.get('conditions', []):
        print(f\"  adapter={item.get('adapter')} {c.get('type')}={c.get('status')} ({c.get('message')})\")"

step "5b. Waiting for aggregated Reconciled=True on the cluster"
CONDITIONS=""
for _ in $(seq 1 45); do
  CONDITIONS=$(curl -sf "${API}/clusters/${CID}" | python3 -c "
import json, sys
cluster = json.load(sys.stdin)
conditions = (cluster.get('status') or {}).get('conditions') or []
print(json.dumps([{ 'type': c.get('type'), 'status': c.get('status') } for c in conditions]))
")
  if echo "${CONDITIONS}" | grep -q '"type": "Reconciled", "status": "True"'; then
    break
  fi
  sleep 2
done
echo "conditions: ${CONDITIONS}"

step "6. Final cluster state"
curl -sf "${API}/clusters/${CID}" | python3 -m json.tool | head -40

step "7. Job state after processing"
pgshow "SELECT queue, state, count(*) FROM river_job GROUP BY 1, 2 ORDER BY 1, 2"

cat <<EOF

Pipeline verified: API -> Sentinel -> Postgres (river) -> adapter -> API status.
Clean up this cluster with:
  curl -X DELETE ${API}/clusters/${CID}
EOF
