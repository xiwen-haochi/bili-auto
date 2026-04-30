#!/usr/bin/env bash
# 触发下载队列（后台异步执行，立即返回）
# 用法：./scripts/download.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ -f "$ENV_FILE" ]]; then
  export $(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | xargs)
fi

SERVICE_HOST="${SERVICE_HOST:-localhost}"
SERVICE_PORT="${SERVICE_PORT:-8000}"
API_KEY="${API_KEY:-}"
BASE_URL="http://${SERVICE_HOST}:${SERVICE_PORT}"

echo "=> 触发下载任务"
curl -s -X POST "${BASE_URL}/download" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Accept: application/json" | python3 -m json.tool

echo ""
echo "=> 当前下载状态"
curl -s -X GET "${BASE_URL}/download/status" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Accept: application/json" | python3 -m json.tool
