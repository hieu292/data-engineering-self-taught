# ═══════════════════════════════════════════════════════════════
#  Phase 6 · Tầng 4 (Catalog) — Unity Catalog OSS
#
#  Thay Hive Metastore độc lập của Phase 5 — chỉ biết "bảng nào ở đâu" —
#  bằng thứ Databricks thật dùng: ba tầng tên (catalog.schema.table) VÀ
#  phân quyền (GRANT/REVOKE) thật, không phải giả lập.
# ═══════════════════════════════════════════════════════════════
FROM unitycatalog/unitycatalog:v0.6.0

USER root

# server.properties.template: bật authorization thật (ảnh gốc mặc định TẮT —
# mọi request đều là admin, GRANT/REVOKE vô nghĩa) + credential MinIO cho
# temporary-credential vending (bắt buộc với bảng EXTERNAL, xem ghi chú trong
# file). hibernate.properties.template: trỏ DB nhúng H2 sang Postgres dùng
# chung. Cả hai đều cần entrypoint.sh điền giá trị thật lúc chạy — .properties
# Java không tự đọc biến môi trường.
COPY conf/server.properties.template /home/unitycatalog/etc/conf/server.properties.template
COPY conf/hibernate.properties.template /home/unitycatalog/etc/conf/hibernate.properties.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && chown unitycatalog:unitycatalog \
       /home/unitycatalog/etc/conf/server.properties.template \
       /home/unitycatalog/etc/conf/hibernate.properties.template

USER unitycatalog
ENTRYPOINT ["/entrypoint.sh"]
CMD ["./bin/start-uc-server"]
