# ═══════════════════════════════════════════════════════════════
#  Phase 5 · Tầng 4 (Catalog, tạm) — Hive Metastore ĐỘC LẬP
#
#  Phase 4 dùng Derby NHÚNG trong chính JVM của spark-connect: một file
#  database chỉ một tiến trình mở được. Trino chạy ở tiến trình khác nên
#  không cách nào chạm vào nó — không phải "chậm", mà là KHÔNG CÓ CỔNG
#  THRIFT nào đang nghe cả.
#
#  Ảnh này chạy Hive Metastore như một SERVICE thật (thrift://:9083), ghi
#  metadata vào Postgres thay vì Derby. Spark và Trino cùng trỏ vào ĐÚNG
#  một service này — lần đầu tiên trong lộ trình catalog không còn thuộc
#  về riêng một engine nào. Đây chính là ý niệm Unity Catalog sẽ thay ở
#  Phase 6, chỉ là chưa có phân quyền/lineage.
# ═══════════════════════════════════════════════════════════════
FROM apache/hive:4.0.0

USER root

# Ảnh gốc không mang sẵn driver JDBC cho Postgres (chỉ có Derby).
# Version lấy từ maven-metadata.xml chính thức, không đoán đại "mới nhất".
ARG POSTGRES_JDBC_VERSION=42.7.13
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgres.jar

# Metastore tự validate LOCATION s3a://... của bảng/database, nên chính nó
# (không chỉ Spark) cần biết đọc s3a://. Ảnh gốc mang Hadoop 3.3.6 — version
# hadoop-aws PHẢI khớp chính xác (bài học cũ của docker/spark/Dockerfile).
# hadoop-aws 3.3.6 khai aws-java-sdk-bundle 1.12.367 trong hadoop-project pom
# — Hadoop 3.3.x còn dùng AWS SDK v1, KHÁC với Hadoop 3.4.2 của ảnh Spark.
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367
ARG MVN=https://repo1.maven.org/maven2
ADD ${MVN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/hive/lib/hadoop-aws.jar
ADD ${MVN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    /opt/hive/lib/aws-java-sdk-bundle.jar

# core-site.xml khai fs.s3a.* — trỏ vào MinIO. HIVE_CUSTOM_CONF_DIR là móc
# nối có sẵn của entrypoint gốc: nó symlink mọi file trong thư mục này vào
# HIVE_CONF_DIR rồi TỰ đặt HADOOP_CONF_DIR trỏ theo, không cần sửa entrypoint.
COPY conf/core-site.xml /opt/hive-custom-conf/core-site.xml
ENV HIVE_CUSTOM_CONF_DIR=/opt/hive-custom-conf

RUN chmod 644 /opt/hive/lib/postgres.jar /opt/hive/lib/hadoop-aws.jar /opt/hive/lib/aws-java-sdk-bundle.jar

USER hive
