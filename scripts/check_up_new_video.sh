#!/usr/bin/env bash
# 检查多个 UP 主是否有新视频动态，有则写入下载队列
# 用法：./scripts/check_up_new_video.sh
# 在下方 UIDS 数组中填写要监控的 UP 主 UID

set -euo pipefail

# =============================================
# 在这里填写要监控的 UP 主 UID（空格分隔）
UIDS=(
  # 123456789
  # 987654321
)
# =============================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ -f "$ENV_FILE" ]]; then
  export $(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | xargs)
fi

SERVICE_HOST="${SERVICE_HOST:-localhost}"
SERVICE_PORT="${SERVICE_PORT:-8000}"
API_KEY="${API_KEY:-}"
BASE_URL="http://${SERVICE_HOST}:${SERVICE_PORT}"

if [[ ${#UIDS[@]} -eq 0 ]]; then
  echo "错误：请在脚本中填写至少一个 UID"
  exit 1
fi

# 拼接 ?uids=xxx&uids=yyy
QUERY=""
for uid in "${UIDS[@]}"; do
  QUERY="${QUERY}&uids=${uid}"
done
QUERY="${QUERY#&}"  # 去掉开头的 &

echo "=> 检查 UP 主新视频动态: ${UIDS[*]}"
curl -s -X GET "${BASE_URL}/check_up_new_video?${QUERY}" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Accept: application/json" | python3 -m json.tool
