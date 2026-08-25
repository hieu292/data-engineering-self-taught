#!/bin/bash
# postgres image tự tạo DB đặt tên theo POSTGRES_DB (= metastore_db, Hive
# Metastore — nơi Spark/dbt/Trino thật sự đọc-ghi bảng) lúc khởi tạo lần đầu.
# Superset và Unity Catalog cần DB metadata RIÊNG — script này chạy một lần
# duy nhất (postgres chỉ thực thi docker-entrypoint-initdb.d/ khi volume dữ
# liệu còn trống), tạo thêm hai DB nữa trong CÙNG một Postgres thay vì dựng
# nhiều container chỉ để tiết kiệm RAM.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "$POSTGRES_SUPERSET_DB";
    CREATE DATABASE "$POSTGRES_UNITY_DB";
EOSQL
