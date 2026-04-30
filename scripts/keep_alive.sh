#!/usr/bin/env bash
# Cookie 保活：50% 概率发送请求，降低行为规律性
# 用法：./scripts/keep_alive.sh

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

# 50% 概率跳过
if (( RANDOM % 2 == 0 )); then
  echo "=> 本次跳过保活（随机）"
  exit 0
fi

echo "=> Cookie 保活"
curl -s -X GET "${BASE_URL}/keep_alive" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Accept: application/json" | python3 -m json.tool
