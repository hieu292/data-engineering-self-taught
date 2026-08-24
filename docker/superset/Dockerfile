# ═══════════════════════════════════════════════════════════════
#  Phase 5 · Tầng 5 (SQL warehouse / BI) — Superset
#
#  Ảnh gốc không có driver cho Postgres (metadata) hay Trino (nguồn dữ
#  liệu BI) — thêm bằng requirements.txt, cùng cách docker/jupyter và
#  docker/dbt đã làm. CMD/entrypoint giữ nguyên của ảnh gốc: nó tự chạy
#  gunicorn qua /usr/bin/run-server.sh, đọc PORT từ biến môi trường.
# ═══════════════════════════════════════════════════════════════
FROM apache/superset:4.1.1

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# PYTHONPATH=/app/pythonpath đã có sẵn trong ảnh gốc — Superset tự import
# superset_config.py từ đây lúc khởi động.
COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
