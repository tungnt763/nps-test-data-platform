#!/usr/bin/env bash
# setup.sh — One-time cluster bootstrap cho NAPAS Data Platform
# Usage: ./scripts/setup.sh [dev|uat|prod]
set -euo pipefail

ENV="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$SCRIPT_DIR/../platform"

echo "═══════════════════════════════════════════════════"
echo " NAPAS Data Platform — Cluster Bootstrap"
echo " Environment: $ENV"
echo "═══════════════════════════════════════════════════"

# ─── Bước 1: Kiểm tra prerequisites ───────────────────
echo ""
echo "▶ [1/6] Kiểm tra prerequisites..."

check_tool() {
  if ! command -v "$1" &>/dev/null; then
    echo "  ✗ $1 chưa được cài. Vui lòng cài $1 trước."
    exit 1
  fi
  echo "  ✓ $1 $(${1} version 2>/dev/null | head -1 || echo '')"
}

check_tool kind
check_tool kubectl
check_tool helm
check_tool helmfile
check_tool docker

# ─── Bước 2: Tạo persistent volume directories ────────
echo ""
echo "▶ [2/6] Tạo persistent volume directories..."

PV_DIRS=(
  "/tmp/napas-platform/worker1/vault"
  "/tmp/napas-platform/worker1/minio"
  "/tmp/napas-platform/worker1/nifi/flow"
  "/tmp/napas-platform/worker1/nifi/content"
  "/tmp/napas-platform/worker1/nifi/provenance"
  "/tmp/napas-platform/worker1/dremio/coordinator"
  "/tmp/napas-platform/worker1/dremio/executor"
  "/tmp/napas-platform/worker2/ranger/postgres"
  "/tmp/napas-platform/worker2/ranger/audit"
  "/tmp/napas-platform/worker2/superset/postgres"
  "/tmp/napas-platform/worker2/superset/redis"
)

for dir in "${PV_DIRS[@]}"; do
  mkdir -p "$dir"
  echo "  ✓ $dir"
done

# ─── Bước 3: Tạo Kind cluster ─────────────────────────
echo ""
echo "▶ [3/6] Tạo Kind cluster 'napas-platform'..."

if kind get clusters 2>/dev/null | grep -q "napas-platform"; then
  echo "  ℹ Cluster 'napas-platform' đã tồn tại. Bỏ qua."
else
  kind create cluster \
    --config "$PLATFORM_DIR/kind-cluster-config.yaml" \
    --wait 120s
  echo "  ✓ Cluster tạo thành công"
fi

kubectl cluster-info --context kind-napas-platform

# ─── Bước 4: Apply namespaces và manifests ────────────
echo ""
echo "▶ [4/6] Apply namespaces và StorageClass..."

kubectl apply -f "$PLATFORM_DIR/manifests/namespaces/namespaces.yaml"
echo "  ✓ 7 namespaces đã tạo"

# StorageClass (local-path provisioner có sẵn trong Kind)
kubectl patch storageclass standard \
  -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}' \
  2>/dev/null || true
echo "  ✓ StorageClass configured"

# ─── Bước 5: Thêm Helm repositories ──────────────────
echo ""
echo "▶ [5/6] Thêm Helm repositories..."

helm repo add hashicorp  https://helm.releases.hashicorp.com
helm repo add minio      https://charts.min.io/
helm repo add bitnami    https://charts.bitnami.com/bitnami
helm repo add dysnix     https://dysnix.github.io/charts
helm repo add superset   https://apache.github.io/superset
helm repo update
echo "  ✓ Helm repos updated"

# ─── Bước 6: Thông báo hoàn thành ────────────────────
echo ""
echo "▶ [6/6] Bootstrap hoàn thành!"
echo ""
echo "  ✅ Cluster: napas-platform (Kind)"
echo "  ✅ 7 namespaces: data-infra → data-visualization"
echo "  ✅ Helm repos: hashicorp, minio, bitnami, dysnix, superset"
echo ""
echo "  Bước tiếp theo — Deploy platform:"
echo "  → Deploy toàn bộ:        ./scripts/deploy.sh $ENV"
echo "  → Deploy từng layer:     ./scripts/deploy.sh $ENV 01-security"
echo ""
echo "  Thứ tự deploy khuyến nghị:"
echo "  1. ./scripts/deploy.sh $ENV 01-security    (Vault)"
echo "  2. ./scripts/deploy.sh $ENV 02-storage     (MinIO)"
echo "  3. ./scripts/deploy.sh $ENV 03-ingestion   (NiFi)"
echo "  4. ./scripts/deploy.sh $ENV 04-governance  (Ranger)"
echo "  5. ./scripts/deploy.sh $ENV 05-processing  (Dremio)"
echo "  6. ./scripts/deploy.sh $ENV 06-visualization (Superset)"
echo ""
echo "  ⚠  SAU KHI DEPLOY VAULT: chạy ngay ./scripts/init-vault.sh $ENV"
