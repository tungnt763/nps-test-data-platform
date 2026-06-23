#!/usr/bin/env bash
# init-vault.sh — Khởi tạo và cấu hình Vault
# Usage: ./scripts/init-vault.sh [dev|prod]
set -u

ENV="${1:-dev}"
VAULT_NS="data-security"
VAULT_POD="vault-0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INIT_FILE="${SCRIPT_DIR}/vault-init-${ENV}.json"

echo "═══════════════════════════════════════════════════"
echo " NAPAS Data Platform — Vault Initialization"
echo " Environment: $ENV"
echo "═══════════════════════════════════════════════════"

# ─── Bước 1: Chờ Vault pod running ──────────────────
echo ""
echo "▶ [1/4] Chờ Vault pod running..."
kubectl wait "pod/$VAULT_POD" -n "$VAULT_NS" \
  --for=jsonpath='{.status.phase}'=Running --timeout=120s
echo "  ✓ Vault pod running"

# ─── Bước 2: Init (bỏ qua nếu đã init) ─────────────
echo ""
echo "▶ [2/4] Init Vault..."

if [[ -f "$INIT_FILE" ]] && [[ -s "$INIT_FILE" ]] && grep -q "unseal_keys_b64" "$INIT_FILE"; then
  echo "  ℹ File $INIT_FILE đã tồn tại với keys hợp lệ — bỏ qua init"
else
  INIT_OUTPUT=$(kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- \
    vault operator init -key-shares=5 -key-threshold=3 -format=json 2>&1)

  if echo "$INIT_OUTPUT" | grep -q "unseal_keys_b64"; then
    echo "$INIT_OUTPUT" > "$INIT_FILE"
    echo "  ✅ Vault initialized — keys saved"
  elif echo "$INIT_OUTPUT" | grep -q "already initialized"; then
    echo "  ℹ Vault đã init trước đó"
    if [[ ! -f "$INIT_FILE" ]] || [[ ! -s "$INIT_FILE" ]]; then
      echo "  ✗ KHÔNG có unseal keys! Cần reset Vault:"
      echo "    helm uninstall vault -n data-security"
      echo "    kubectl delete pvc --all -n data-security"
      echo "    sleep 10"
      echo "    ./scripts/deploy.sh dev 01-security"
      echo "    sleep 30 && ./scripts/init-vault.sh dev"
      exit 1
    fi
  else
    echo "  ✗ Init thất bại: $INIT_OUTPUT"
    exit 1
  fi
fi

# ─── Bước 3: Unseal ─────────────────────────────────
echo ""
echo "▶ [3/4] Unseal Vault..."

KEY1=$(sed -n '3p' "$INIT_FILE" | tr -d ' ",')
KEY2=$(sed -n '4p' "$INIT_FILE" | tr -d ' ",')
KEY3=$(sed -n '5p' "$INIT_FILE" | tr -d ' ",')

echo "  Key1: ${KEY1:0:8}..."
echo "  Key2: ${KEY2:0:8}..."
echo "  Key3: ${KEY3:0:8}..."

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault operator unseal "$KEY1" > /dev/null 2>&1
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault operator unseal "$KEY2" > /dev/null 2>&1
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault operator unseal "$KEY3" > /dev/null 2>&1

# Verify — kiểm tra pod READY (1/1 = unsealed)
sleep 3
READY=$(kubectl get pod "$VAULT_POD" -n "$VAULT_NS" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
if [[ "$READY" == "true" ]]; then
  echo "  ✓ Vault unsealed (pod 1/1 Ready)"
else
  echo "  ✗ Vault vẫn sealed (pod 0/1)"
  echo "  Debug: kubectl exec -n $VAULT_NS $VAULT_POD -- vault status"
  exit 1
fi

# ─── Bước 4: Cấu hình secrets ────────────────────────
echo ""
echo "▶ [4/4] Cấu hình secrets..."

ROOT_TOKEN=$(grep '"root_token"' "$INIT_FILE" | tr -d ' ",' | cut -d: -f2)
echo "  Token: ${ROOT_TOKEN:0:10}..."

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault login "$ROOT_TOKEN" > /dev/null 2>&1

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault secrets enable -path=napas kv-v2 > /dev/null 2>&1 || true
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault auth enable kubernetes > /dev/null 2>&1 || true
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443" > /dev/null 2>&1
echo "  ✓ KV engine + K8s auth"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault kv put napas/minio/credentials \
  access_key="napas-admin" secret_key="napas-minio-s3cr3t-2024" > /dev/null 2>&1
echo "  ✓ napas/minio/credentials"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault kv put napas/ranger/postgres \
  host="postgres-ranger-postgresql.data-governance.svc.cluster.local" \
  port="5432" database="ranger" username="ranger" password="Ranger@Postgres2024" > /dev/null 2>&1
echo "  ✓ napas/ranger/postgres"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault kv put napas/superset/config \
  secret_key="napas-superset-secret-key-2024" \
  postgres_host="postgres-superset-postgresql.data-visualization.svc.cluster.local" \
  postgres_password="Superset@Postgres2024" > /dev/null 2>&1
echo "  ✓ napas/superset/config"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault kv put napas/dremio/admin \
  username="admin" password="Dremio@Admin2024" > /dev/null 2>&1
echo "  ✓ napas/dremio/admin"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- sh -c 'vault policy write nifi-policy - <<POLICY
path "napas/data/minio/*" { capabilities = ["read"] }
path "napas/data/nifi/*"  { capabilities = ["read"] }
POLICY' > /dev/null 2>&1

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- sh -c 'vault policy write dremio-policy - <<POLICY
path "napas/data/minio/*"  { capabilities = ["read"] }
path "napas/data/dremio/*" { capabilities = ["read"] }
POLICY' > /dev/null 2>&1

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- sh -c 'vault policy write ranger-policy - <<POLICY
path "napas/data/ranger/*" { capabilities = ["read"] }
POLICY' > /dev/null 2>&1

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- sh -c 'vault policy write superset-policy - <<POLICY
path "napas/data/superset/*" { capabilities = ["read"] }
POLICY' > /dev/null 2>&1
echo "  ✓ 4 policies"

kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault write auth/kubernetes/role/nifi-role \
  bound_service_account_names=nifi bound_service_account_namespaces=data-ingestion \
  policies=nifi-policy ttl=1h > /dev/null 2>&1
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault write auth/kubernetes/role/dremio-role \
  bound_service_account_names=dremio bound_service_account_namespaces=data-processing \
  policies=dremio-policy ttl=1h > /dev/null 2>&1
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault write auth/kubernetes/role/ranger-role \
  bound_service_account_names=ranger bound_service_account_namespaces=data-governance \
  policies=ranger-policy ttl=1h > /dev/null 2>&1
kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- vault write auth/kubernetes/role/superset-role \
  bound_service_account_names=superset bound_service_account_namespaces=data-visualization \
  policies=superset-policy ttl=1h > /dev/null 2>&1
echo "  ✓ 4 K8s roles"

# Validate
READBACK=$(kubectl exec -n "$VAULT_NS" "$VAULT_POD" -- \
  vault kv get -field=access_key napas/minio/credentials 2>/dev/null || echo "FAIL")

if [[ "$READBACK" == "napas-admin" ]]; then
  echo "  ✓ Secret read-back: OK"
else
  echo "  ✗ Secret read-back: FAIL"
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo " ✅ Vault Hoàn Thành!"
echo "═══════════════════════════════════════════════════"
echo " Next: ./scripts/deploy.sh dev 02-storage"
