#!/usr/bin/env bash
# 上传脚本：cd 到项目目录后执行 uploader.py
# 用法：./scripts/upload.sh

set -euo pipefail

SUB_DIR="./downloads"

cd /home/ubuntu/auto-bili

echo "=> 上传目录：$SUB_DIR"
/home/ubuntu/auto-bili/.venv/bin/python uploader.py --dir "$SUB_DIR"
