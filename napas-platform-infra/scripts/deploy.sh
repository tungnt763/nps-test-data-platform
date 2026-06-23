#!/usr/bin/env bash
# deploy.sh — Deploy/upgrade các components của NAPAS Data Platform
# Usage: ./scripts/deploy.sh [env] [layer] [action]
#
# Examples:
#   ./scripts/deploy.sh dev                      # Deploy toàn bộ cho dev
#   ./scripts/deploy.sh dev 01-security          # Deploy chỉ Vault
#   ./scripts/deploy.sh dev 02-storage           # Deploy chỉ MinIO
#   ./scripts/deploy.sh prod "" diff             # Preview changes cho prod
#   ./scripts/deploy.sh dev "" destroy           # Xóa toàn bộ (cẩn thận!)
set -euo pipefail

ENV="${1:-dev}"
LAYER="${2:-}"
ACTION="${3:-sync}"   # sync | diff | destroy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$SCRIPT_DIR/../platform"

echo "═══════════════════════════════════════════════════"
echo " NAPAS Data Platform — Deploy"
echo " Env: $ENV | Layer: ${LAYER:-ALL} | Action: $ACTION"
echo "═══════════════════════════════════════════════════"

# Validate environment
if [[ ! -f "$PLATFORM_DIR/environments/$ENV.yaml" ]]; then
  echo "✗ Environment '$ENV' không tồn tại."
  echo "  Các env hợp lệ: dev, uat, prod"
  exit 1
fi

# Build helmfile command
HELMFILE_CMD="helmfile --environment $ENV -f $PLATFORM_DIR/helmfile.yaml.gotmpl"

# Filter by layer nếu chỉ định
if [[ -n "$LAYER" ]]; then
  HELMFILE_CMD="$HELMFILE_CMD --selector layer=$LAYER"
fi

# Execute action
case "$ACTION" in
  sync)
    echo ""
    echo "▶ Deploying..."
    $HELMFILE_CMD sync --concurrency 1
    ;;
  diff)
    echo ""
    echo "▶ Diffing (preview only)..."
    $HELMFILE_CMD diff
    ;;
  destroy)
    echo ""
    echo "⚠  WARNING: Sắp XÓA toàn bộ releases cho env=$ENV layer=${LAYER:-ALL}"
    read -p "  Xác nhận (yes/no): " CONFIRM
    if [[ "$CONFIRM" == "yes" ]]; then
      $HELMFILE_CMD destroy
    else
      echo "  Hủy."
      exit 0
    fi
    ;;
  *)
    echo "✗ Action '$ACTION' không hợp lệ. Dùng: sync | diff | destroy"
    exit 1
    ;;
esac

echo ""
echo "✅ Done — $ACTION $ENV ${LAYER:-ALL}"
