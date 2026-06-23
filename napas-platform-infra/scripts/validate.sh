#!/usr/bin/env bash
# validate.sh — Kiểm tra toàn bộ platform hoạt động end-to-end
# Usage: ./scripts/validate.sh [dev|prod] [component]
#
# Examples:
#   ./scripts/validate.sh dev          # Validate toàn bộ
#   ./scripts/validate.sh dev vault    # Chỉ validate Vault
#   ./scripts/validate.sh dev minio    # Chỉ validate MinIO
set -euo pipefail

ENV="${1:-dev}"
COMPONENT="${2:-all}"

PASS=0
FAIL=0
SKIP=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

check() {
  local name="$1"
  local cmd="$2"
  local expected="${3:-}"

  printf "  %-50s" "$name"
  if result=$(eval "$cmd" 2>/dev/null); then
    if [[ -z "$expected" ]] || echo "$result" | grep -q "$expected"; then
      echo -e "${GREEN}✓ PASS${NC}"
      ((PASS++))
    else
      echo -e "${RED}✗ FAIL${NC} (expected: $expected, got: $result)"
      ((FAIL++))
    fi
  else
    echo -e "${RED}✗ FAIL${NC} (command failed)"
    ((FAIL++))
  fi
}

check_pod_ready() {
  local ns="$1"
  local label="$2"
  local name="$3"
  check "$name" \
    "kubectl get pods -n $ns -l $label -o jsonpath='{.items[0].status.containerStatuses[0].ready}'" \
    "true"
}

echo "═══════════════════════════════════════════════════"
echo " NAPAS Data Platform — Validation"
echo " Env: $ENV | Component: $COMPONENT"
echo "═══════════════════════════════════════════════════"

# ─── Vault ────────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "vault" ]]; then
  echo ""
  echo "▶ [1/6] HashiCorp Vault (data-security)"
  check_pod_ready "data-security" "app.kubernetes.io/name=vault" "  Pod vault-0 ready"
  check "  Vault unsealed" \
    "kubectl exec -n data-security vault-0 -- vault status -format=json" \
    '"sealed":false'
  check "  Secret napas/minio readable" \
    "kubectl exec -n data-security vault-0 -- vault kv get -format=json napas/minio/credentials" \
    "access_key"
fi

# ─── MinIO ────────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "minio" ]]; then
  echo ""
  echo "▶ [2/6] MinIO (data-storage)"
  check_pod_ready "data-storage" "app=minio" "  Pod minio ready"

  # Start port-forward
  kubectl port-forward -n data-storage svc/minio 9000:9000 &>/dev/null &
  PF_MINIO=$!
  sleep 2
  trap "kill $PF_MINIO 2>/dev/null || true" EXIT

  check "  S3 API health check" \
    "curl -sf http://localhost:9000/minio/health/live" \
    ""
  check "  Bucket napas-datalake exists" \
    "mc ls http://localhost:9000 --access-key napas-admin --secret-key napas-minio-s3cr3t-2024 2>/dev/null || kubectl exec -n data-storage deploy/minio -- mc ls local/napas-datalake" \
    ""
fi

# ─── NiFi ─────────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "nifi" ]]; then
  echo ""
  echo "▶ [3/6] Apache NiFi (data-ingestion)"
  check_pod_ready "data-ingestion" "app=nifi" "  Pod nifi-0 ready"

  kubectl port-forward -n data-ingestion svc/nifi 8443:8443 &>/dev/null &
  PF_NIFI=$!
  sleep 2

  check "  NiFi API accessible" \
    "curl -sk https://localhost:8443/nifi-api/system-diagnostics | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[\"systemDiagnostics\"][\"aggregateSnapshot\"][\"activeThreadCount\"])'" \
    ""
fi

# ─── Ranger ───────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "ranger" ]]; then
  echo ""
  echo "▶ [4/6] Apache Ranger (data-governance)"
  check_pod_ready "data-governance" "app=postgres-ranger" "  Pod postgres-ranger ready"
  check_pod_ready "data-governance" "app=ranger" "  Pod ranger ready"

  kubectl port-forward -n data-governance svc/ranger 6080:6080 &>/dev/null &
  PF_RANGER=$!
  sleep 2

  check "  Ranger Admin API health" \
    "curl -sf -u admin:RangerAdmin@2024 http://localhost:6080/service/public/v2/api/servicedef" \
    "serviceName"
fi

# ─── Dremio ───────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "dremio" ]]; then
  echo ""
  echo "▶ [5/6] Dremio (data-processing)"
  check_pod_ready "data-processing" "app=dremio,role=coordinator" "  Pod dremio-coordinator ready"
  check_pod_ready "data-processing" "app=dremio,role=executor" "  Pod dremio-executor ready"

  kubectl port-forward -n data-processing svc/dremio 9047:9047 &>/dev/null &
  PF_DREMIO=$!
  sleep 3

  TOKEN=$(curl -sf -X POST http://localhost:9047/apiv2/login \
    -H "Content-Type: application/json" \
    -d '{"userName":"admin","password":"Dremio@Admin2024"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null || echo "")

  check "  Dremio REST API login" \
    "echo '$TOKEN' | grep -c ." \
    "1"
  check "  Dremio SQL SELECT 1" \
    "curl -sf -X POST http://localhost:9047/apiv2/sql \
      -H 'Authorization: _dremio$TOKEN' \
      -H 'Content-Type: application/json' \
      -d '{\"sql\":\"SELECT 1 as test\"}' | python3 -c 'import sys,json; j=json.load(sys.stdin); print(j.get(\"rows\",[[]])[0][0] if j.get(\"rows\") else \"ok\")'" \
    ""
fi

# ─── Superset ─────────────────────────────────────────
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "superset" ]]; then
  echo ""
  echo "▶ [6/6] Apache Superset (data-visualization)"
  check_pod_ready "data-visualization" "app=superset" "  Pod superset ready"
  check_pod_ready "data-visualization" "app=superset-worker" "  Pod superset-worker ready"
  check_pod_ready "data-visualization" "app=superset-redis" "  Pod redis ready"

  kubectl port-forward -n data-visualization svc/superset 8088:8088 &>/dev/null &
  PF_SUPERSET=$!
  sleep 2

  check "  Superset health endpoint" \
    "curl -sf http://localhost:8088/health" \
    "OK"
fi

# ─── Summary ──────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo " Validation Summary"
echo "═══════════════════════════════════════════════════"
echo -e " ${GREEN}PASS${NC}: $PASS"
echo -e " ${RED}FAIL${NC}: $FAIL"
echo " SKIP: $SKIP"
echo ""

if [[ $FAIL -eq 0 ]]; then
  echo -e " ${GREEN}🎉 Tất cả checks PASS — Platform hoạt động bình thường!${NC}"
  exit 0
else
  echo -e " ${RED}⚠ $FAIL checks FAIL — Xem logs để debug.${NC}"
  echo ""
  echo " Debug commands:"
  echo " kubectl logs -n data-security vault-0"
  echo " kubectl logs -n data-storage \$(kubectl get pod -n data-storage -l app=minio -o name)"
  echo " kubectl logs -n data-ingestion nifi-0"
  echo " kubectl logs -n data-governance \$(kubectl get pod -n data-governance -l app=ranger -o name)"
  echo " kubectl logs -n data-processing \$(kubectl get pod -n data-processing -l role=coordinator -o name)"
  exit 1
fi
