#!/bin/sh
# Java .properties không tự đọc biến môi trường — thay placeholder bằng giá
# trị thật trước khi server đọc file, mỗi lần container khởi động (không chỉ
# lúc build, vì mật khẩu Postgres/MinIO là bí mật runtime, không phải build-time).
set -eu

sed \
  -e "s|__POSTGRES_UNITY_DB__|${POSTGRES_UNITY_DB}|g" \
  -e "s|__POSTGRES_USER__|${POSTGRES_USER}|g" \
  -e "s|__POSTGRES_PASSWORD__|${POSTGRES_PASSWORD}|g" \
  /home/unitycatalog/etc/conf/hibernate.properties.template \
  > /home/unitycatalog/etc/conf/hibernate.properties

sed \
  -e "s|__MINIO_BUCKET__|${MINIO_BUCKET}|g" \
  -e "s|__MINIO_ROOT_USER__|${MINIO_ROOT_USER}|g" \
  -e "s|__MINIO_ROOT_PASSWORD__|${MINIO_ROOT_PASSWORD}|g" \
  /home/unitycatalog/etc/conf/server.properties.template \
  > /home/unitycatalog/etc/conf/server.properties

exec "$@"
