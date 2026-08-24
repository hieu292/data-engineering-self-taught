# ═══════════════════════════════════════════════════════════════
#  Phase 5 · Tầng 5 (SQL warehouse / BI) — cấu hình Superset
#
#  Nạp qua PYTHONPATH (/app/pythonpath, biến sẵn có trong image gốc).
#  Superset tự import file này lúc khởi động vì tên đúng "superset_config".
# ═══════════════════════════════════════════════════════════════
import os

# Bắt buộc phải có — Superset 4.x từ chối khởi động nếu SECRET_KEY rỗng
# hoặc dùng giá trị mặc định của image mẫu.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata DB của Superset (dashboard, chart, user...) — dùng chung một
# Postgres với Hive Metastore, khác database, để khỏi tốn thêm một
# container chỉ để lưu vài bảng cấu hình.
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_NAME = os.environ["POSTGRES_SUPERSET_DB"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@postgres:5432/{DB_NAME}"
)
