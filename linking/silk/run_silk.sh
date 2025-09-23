#!/usr/bin/env bash
set -euo pipefail

CONFIG=/home/nghianv/workspace/semantic-web-football-kg/linking/silk/config.xml
OUTPUT_DIR=/home/nghianv/workspace/semantic-web-football-kg/linking/links
mkdir -p "$OUTPUT_DIR"

# Dùng image Silk Workbench/CLI (silkframework/silk-workbench) để chạy job CLI
# Lưu ý: Wikidata áp rate limit, có thể cần sleep/retry nếu chạy nhiều

docker run --rm \
  -v /home/nghianv/workspace/semantic-web-football-kg/linking/silk:/silk \
  -v /home/nghianv/workspace/semantic-web-football-kg/linking/links:/links \
  -e JAVA_OPTS="-Xmx2g" \
  --network host \
  silkframework/silk-workbench:latest \
  /opt/silk/bin/silk -Dconfig.file=/silk/config.xml

echo "Silk finished. Check links in $OUTPUT_DIR"
