#!/usr/bin/env bash
# Tải dữ liệu NYC Yellow Taxi. Mỗi file ~50MB, khoảng 3 triệu chuyến/tháng.
set -euo pipefail

MONTHS=("${@:-2024-01 2024-02 2024-03}")
BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
DEST="$(cd "$(dirname "$0")/.." && pwd)/data"
mkdir -p "$DEST"

for m in ${MONTHS[@]}; do
  f="yellow_tripdata_${m}.parquet"
  if [[ -f "$DEST/$f" ]]; then
    echo "→ đã có, bỏ qua: $f"
  else
    echo "↓ đang tải: $f"
    curl -fL --progress-bar -o "$DEST/$f" "$BASE/$f"
  fi
done

echo ""
echo "✓ Xong. Dữ liệu trong $DEST:"
du -h "$DEST"/*.parquet 2>/dev/null || true
