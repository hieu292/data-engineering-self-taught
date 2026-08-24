#!/bin/bash
# postgres image tự tạo DB đặt tên theo POSTGRES_DB (= metastore_db) lúc
# khởi tạo lần đầu. Superset cần một DB metadata RIÊNG — script này chạy
# một lần duy nhất (postgres chỉ thực thi docker-entrypoint-initdb.d/ khi
# volume dữ liệu còn trống), tạo thêm DB thứ hai trong CÙNG một Postgres
# thay vì dựng nguyên một container Postgres nữa chỉ để tiết kiệm RAM.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "$POSTGRES_SUPERSET_DB";
EOSQL
