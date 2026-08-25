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

# unitycatalog-spark_4.1_2.13 — CHÚ Ý cái "_4.1_" giữa tên: khác artifact hẳn
# với unitycatalog-spark_2.13 (không có số Spark). Tìm nhầm bản không có số
# Spark (build cho spark-sql 4.0.0, lệch với 4.1.3 của image này) là bài học
# đầu — nhưng chưa đủ: CREATE TABLE vẫn vỡ với
# "java.lang.NoClassDefFoundError: io/unitycatalog/client/delta/model/DeltaTableUpdate"
# dù đã dùng đúng bản _4.1_. Lớp đó nằm trong unitycatalog-client, và chỉ xuất
# hiện đủ khi Ivy giải luôn CẢ delta-kernel-unitycatalog (UC Delta API — Delta
# 4.3.1 dùng để tạo/commit bảng qua catalog thay vì ghi thẳng file). Bài học
# thật: `unitycatalog-spark` một mình không đủ, phải kéo theo mô-đun Delta
# phía UC cần, và version PHẢI khớp `${DELTA_VERSION}` ở trên.
ARG UC_SPARK_VERSION=0.6.0

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

# unitycatalog-spark KHÔNG tự tay curl như trên được: nó kéo theo ~25 jar phụ
# thuộc bắc cầu (jackson, antlr4, hadoop-client, log4j...) — dò tay chắc chắn
# thiếu sót. Thay vào đó, dùng chính `--packages` của Spark để Ivy giải đúng
# cây phụ thuộc, nhưng chạy LÚC BUILD (nhắm một class không tồn tại — Ivy
# resolve xong TRƯỚC khi thử nạp class, nên vẫn lấy được jar dù lệnh báo lỗi
# ở bước sau) rồi copy jar ra /opt/spark/jars. Runtime vẫn không cần mạng.
#
# Đã kiểm tra bằng tay: image gốc SẴN CÓ bản MỚI HƠN của jackson-*, log4j-*,
# slf4j-api, snappy-java, hadoop-client-api/runtime (dò `ls /opt/spark/jars`
# rồi so với jar Ivy resolve ra) — copy đè bản CŨ hơn Ivy kéo về sẽ tạo ra hai
# bản trùng tên khác version trên classpath, đúng loại lỗi "chạy được ở chỗ
# này, vỡ ở chỗ khác" dự án này đã né được với hadoop-aws. Chỉ copy đúng
# những jar image gốc CHƯA có.
RUN set -eux; \
    /opt/spark/bin/spark-submit \
      --packages "io.unitycatalog:unitycatalog-spark_4.1_2.13:${UC_SPARK_VERSION},io.delta:delta-kernel-unitycatalog:${DELTA_VERSION}" \
      --conf spark.jars.ivy=/opt/spark/.ivy2 \
      --class does.not.Exist \
      /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar > /tmp/uc-resolve.log 2>&1 || true; \
    grep -q "Failed to load class" /tmp/uc-resolve.log \
      || (cat /tmp/uc-resolve.log; echo "FAIL: spark-submit không tới được bước nạp class — Ivy resolve có thể đã lỗi"; exit 1); \
    cd /opt/spark/.ivy2/jars; \
    for jar in \
      io.unitycatalog_unitycatalog-spark_4.1_2.13-${UC_SPARK_VERSION}.jar \
      io.unitycatalog_unitycatalog-client-${UC_SPARK_VERSION}.jar \
      io.unitycatalog_unitycatalog-hadoop-${UC_SPARK_VERSION}.jar \
      io.delta_delta-kernel-unitycatalog-${DELTA_VERSION}.jar \
      com.ibm.icu_icu4j-72.1.jar \
      commons-logging_commons-logging-1.3.0.jar \
      org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar \
      org.antlr_ST4-4.3.4.jar \
      org.antlr_antlr4-4.13.1.jar \
      com.fasterxml.jackson.dataformat_jackson-dataformat-xml-2.15.0.jar \
      org.openapitools_jackson-databind-nullable-0.2.6.jar \
      com.thoughtworks.paranamer_paranamer-2.8.jar \
      com.fasterxml.woodstox_woodstox-core-6.5.1.jar \
      org.codehaus.woodstox_stax2-api-4.2.1.jar \
      com.google.code.findbugs_jsr305-3.0.2.jar \
    ; do \
      cp "$jar" /opt/spark/jars/; \
    done; \
    chmod -R a+rX /opt/spark/jars; \
    rm -rf /opt/spark/.ivy2 /tmp/uc-resolve.log

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
