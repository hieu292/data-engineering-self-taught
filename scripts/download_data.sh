#!/usr/bin/env bash
# Tải dữ liệu NYC Yellow Taxi. Mỗi file ~50MB, khoảng 3 triệu chuyến/tháng.
set -euo pipefail

# Mặc định: cả 12 tháng 2024 (~36 triệu chuyến, ~600MB).
# Phase 0-2 chỉ cần 1 tháng; Phase 3 cần lượng này thì shuffle mới đủ
# tốn thời gian để nhìn thấy chênh lệch khi tối ưu.
DEFAULT_MONTHS="2024-01 2024-02 2024-03 2024-04 2024-05 2024-06 \
                2024-07 2024-08 2024-09 2024-10 2024-11 2024-12"
MONTHS=("${@:-$DEFAULT_MONTHS}")
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

# Bảng tra vùng đón/trả (265 dòng, ~12KB). Phase 3 cần nó để có một bảng
# NHỎ đem join với bảng lớn — tình huống broadcast join kinh điển.
if [[ ! -f "$DEST/taxi_zone_lookup.csv" ]]; then
  echo "↓ đang tải: taxi_zone_lookup.csv"
  curl -fL --progress-bar -o "$DEST/taxi_zone_lookup.csv" \
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
else
  echo "→ đã có, bỏ qua: taxi_zone_lookup.csv"
fi

echo ""
echo "✓ Xong. Dữ liệu trong $DEST:"
du -h "$DEST"/*.parquet 2>/dev/null || true
