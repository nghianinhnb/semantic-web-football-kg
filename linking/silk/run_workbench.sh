#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_HOST=/home/nghianv/workspace/semantic-web-football-kg/linking/silk
LINKS_HOST=/home/nghianv/workspace/semantic-web-football-kg/linking/links

mkdir -p "$WORKSPACE_HOST" "$LINKS_HOST"

# Dừng container cũ nếu có
if docker ps -a --format '{{.Names}}' | grep -q '^silk-workbench$'; then
  docker rm -f silk-workbench >/dev/null 2>&1 || true
fi

# Chạy workbench
# - Map workspace sang /opt/silk/workspace
# - Dùng --network host để truy cập Fuseki trên host
# - Output file được cấu hình tới /opt/silk/workspace/links/...

docker run -d --name silk-workbench \
  --network host \
  -v "$WORKSPACE_HOST":/opt/silk/workspace \
  -v "$LINKS_HOST":/opt/silk/workspace/links \
  silkworkbench/silk-framework:latest

echo "Silk Workbench started at http://localhost:9000"
