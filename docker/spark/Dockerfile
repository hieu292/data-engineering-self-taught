# ═══════════════════════════════════════════════════════════════
#  Phase 3 · Tầng 3 (Compute engine) — Apache Spark
#  Databricks gọi tầng này là "Databricks Runtime".
#
#  MỘT image dùng chung cho cả 4 vai: master, worker, connect server.
#  Vai nào là do `command` trong docker-compose.yml quyết định, không
#  phải do image. Đó cũng đúng cách Spark thật hoạt động: cùng một bộ
#  binary, khác nhau ở tiến trình được khởi động.
# ═══════════════════════════════════════════════════════════════
FROM apache/spark:4.1.3

USER root

# ── Version: KHÔNG phỏng đoán. Mỗi số đều có căn cứ. ──────────────
#
#   HADOOP_VERSION  phải khớp CHÍNH XÁC hadoop-client-*.jar có sẵn trong
#                   image (đã kiểm: 3.4.2). Lệch một số nhỏ là NoSuchMethodError
#                   lúc runtime, không phải lúc build — kiểu lỗi khó chịu nhất.
#
#   AWS_SDK_VERSION lấy từ <aws-java-sdk-v2.version> trong pom của
#                   hadoop-project 3.4.2. Hadoop 3.4 đã chuyển sang AWS SDK **v2**;
#                   các hướng dẫn cũ trên mạng còn dạy `aws-java-sdk-bundle`
#                   (SDK v1) — dùng nhầm sẽ ra ClassNotFoundException.
#
#   DELTA_VERSION   4.3.1 chứ KHÔNG phải 4.4.0 mới nhất. Pom của delta 4.4.0
#                   khai spark-sql 4.2.0, còn 4.3.1 khai 4.1.0 — khớp image này.
#                   "Mới nhất" và "đúng" là hai chuyện khác nhau.
#
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG DELTA_VERSION=4.3.1

ARG MVN=https://repo1.maven.org/maven2

# Tải jar vào lúc BUILD, không dùng `--packages` lúc chạy. Hai lý do:
# cluster khởi động không cần mạng, và mọi worker chắc chắn có cùng một bộ jar
# — nguồn gốc kinh điển của lỗi "chạy được trên driver, chết trên executor".
#
# Cảnh báo trung thực: bundle-*.jar nặng ~611MB. Đó là toàn bộ AWS SDK v2 gộp
# một cục. Đổi lại là khỏi phải dò 20 jar con và trùng version. Build một lần.
RUN set -eux; \
    cd /opt/spark/jars; \
    for url in \
      "${MVN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "${MVN}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
      "${MVN}/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
      "${MVN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "${MVN}/io/delta/delta-connect-server_2.13/${DELTA_VERSION}/delta-connect-server_2.13-${DELTA_VERSION}.jar" \
      "${MVN}/io/delta/delta-connect-common_2.13/${DELTA_VERSION}/delta-connect-common_2.13-${DELTA_VERSION}.jar" \
    ; do \
      echo "→ $(basename "$url")"; \
      curl -fsSL --retry 3 -O "$url"; \
    done

# ── Vá lệch version Python giữa client và executor ─────────────
#
# Image này mang sẵn Python 3.10 (nền Ubuntu 22.04), còn container notebook
# chạy Python 3.13. Spark Connect ĐỐI CHIẾU major.minor của hai đầu và ném
# lỗi ngay khi gặp UDF Python đầu tiên — vì hàm được pickle ở client rồi
# unpickle ở executor, khác version là khác định dạng bytecode.
#
# Đây không phải chuyện riêng của dự án này: Databricks cũng bắt version
# Databricks Connect phải khớp Databricks Runtime, vì đúng lý do đó.
#
# Cách chữa: cài thêm một Python 3.13 độc lập vào image và trỏ PYSPARK_PYTHON
# vào nó. Executor sẽ chạy Python worker bằng bản này, khớp với notebook.
#
# Hai chi tiết về quyền, cả hai đều chỉ nổ ra lúc gặp UDF ĐẦU TIÊN chứ không
# phải lúc build — nên rất dễ mất thời gian:
#   • UV_PYTHON_INSTALL_DIR: mặc định uv cài Python vào /root (mode 700).
#     Executor chạy bằng user `spark`, không vào nổi. Phải đưa ra /opt.
#   • chmod a+rX: lệnh RUN này chạy bằng root, file sinh ra không có quyền
#     đọc/chạy cho user khác.
# Triệu chứng của cả hai giống hệt nhau: "Cannot run program ... Permission denied".
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python

RUN set -eux; \
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh; \
    uv python install 3.13; \
    uv venv --python 3.13 /opt/venv313; \
    uv pip install --python /opt/venv313/bin/python --no-cache pyarrow pandas; \
    chmod -R a+rX /opt/uv-python /opt/venv313; \
    /opt/venv313/bin/python -V; \
    test -x "$(readlink -f /opt/venv313/bin/python)"

ENV PYSPARK_PYTHON=/opt/venv313/bin/python

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Thư mục Spark ghi log sự kiện và file shuffle tràn đĩa
RUN mkdir -p /opt/spark/logs /opt/spark/work && chmod -R 777 /opt/spark/logs /opt/spark/work

USER spark
