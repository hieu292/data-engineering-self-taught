# ═══════════════════════════════════════════════════════════════
#  Phase 4 · Tầng 6 (Transform) — dbt
#
#  dbt chạy trong container RIÊNG, không nhét vào image Jupyter. Ba lý do:
#    1. dbt là một công cụ độc lập, không phải thư viện của notebook.
#    2. Dependency của nó (Jinja, agate, protobuf...) tách bạch khỏi notebook —
#       đúng bài học C2 của Phase 0 về xung đột version.
#    3. Phase 7 Airflow sẽ gọi thẳng container này. Dựng sẵn từ bây giờ.
#
#  Container này KHÔNG tính toán gì. Nó biên dịch SQL rồi gửi qua gRPC cho
#  driver ở spark-connect — đúng driver mà notebook đang dùng. Nghĩa là bảng
#  dbt tạo ra, notebook nhìn thấy ngay, không cần đồng bộ gì.
# ═══════════════════════════════════════════════════════════════
FROM python:3.13-slim

# procps chỉ để có `pkill` — `make dbt-docs` dùng nó dọn tiến trình serve cũ
# trước khi mở cái mới, nếu không lần chạy thứ hai sẽ đụng "Address already in use".
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# dbt viết log và artifact (manifest.json, run_results.json) vào target/.
# Thư mục dự án được mount từ host nên các file này xem được từ ngoài.
WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt

# Container thường trực để `docker compose exec` vào chạy lệnh dbt bất kỳ.
CMD ["sleep", "infinity"]
