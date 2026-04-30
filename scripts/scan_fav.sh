#!/usr/bin/env bash
# 扫描收藏夹并将新视频入队
# 用法：
#   ./scripts/scan_fav.sh              # 扫描全部收藏夹
#   ./scripts/scan_fav.sh "我的收藏"   # 扫描指定收藏夹

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

# 加载 .env
if [[ -f "$ENV_FILE" ]]; then
  # 忽略注释行和空行
  export $(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | xargs)
fi

SERVICE_HOST="${SERVICE_HOST:-localhost}"
SERVICE_PORT="${SERVICE_PORT:-8000}"
API_KEY="${API_KEY:-}"
BASE_URL="http://${SERVICE_HOST}:${SERVICE_PORT}"

FOLDER_NAME="${1:-测试收藏}"

if [[ -n "$FOLDER_NAME" ]]; then
  URL="${BASE_URL}/scan_fav?folder_name=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$FOLDER_NAME")"
else
  URL="${BASE_URL}/scan_fav"
fi

echo "=> 扫描收藏夹: ${FOLDER_NAME:-（全部）}"
curl -s -X GET "$URL" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Accept: application/json" | python3 -m json.tool
