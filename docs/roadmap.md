# Lộ trình Data Engineering — Clone Databricks bằng Open Source

> Tài liệu sống. Cập nhật khi hoàn thành từng phase.

## Bối cảnh

| Mục | Giá trị |
|---|---|
| Nền tảng có sẵn | SQL ổn, Python ổn, Docker cơ bản |
| Vùng trắng | Spark, big data, lakehouse |
| Mục tiêu | Xin việc Data Engineer |
| Nhịp học | 5-8h/tuần (buổi tối) |
| Phạm vi | Lakehouse + orchestration + **streaming** + **ML** |
| Thời lượng dự kiến | ~7-9 tháng |
| Môi trường | macOS Apple Silicon (arm64), 32GB RAM, Docker 29 / Compose v5 |

**Cách tiếp cận:** lát cắt mỏng trước, rồi đào sâu theo tầng. Phase 0 dựng ngay một pipeline
chạy đầu-cuối để có bản đồ tư duy. Từ Phase 1, mỗi phase thay một mảnh "đồ chơi" bằng "đồ thật"
và **luôn kết thúc bằng một pipeline chạy được** — không bao giờ là một service đứng lẻ.

Việc viết lại code nhiều lần ở đây không lãng phí: chính lúc thay DuckDB bằng Spark và thấy nó
khác chỗ nào, đó là lúc thực sự hiểu Spark sinh ra để làm gì.

---

## Phần 1 — Kiến trúc và ánh xạ "Databricks → OSS"

Databricks không phải một sản phẩm, nó là **7 tầng xếp chồng**. Nắm được 7 tầng này là nắm
được kiến trúc data platform hiện đại nói chung, không chỉ riêng Databricks.

| # | Tầng | Databricks gọi là | Ta dùng | Giải quyết vấn đề gì |
|---|---|---|---|---|
| 1 | Object storage | DBFS / S3 | **MinIO** | Chứa file thô, rẻ, vô hạn — nhưng không có transaction |
| 2 | Table format | **Delta Lake** | **Delta Lake** *(chính hãng)* | ACID, time-travel, schema evolution trên đống file ở tầng 1 |
| 3 | Compute engine | Databricks Runtime | **Apache Spark 4** | Xử lý song song, tính toán trên dữ liệu lớn hơn RAM |
| 4 | Catalog / governance | **Unity Catalog** | **Unity Catalog OSS** *(chính hãng)* | "Có bảng nào, ai được đọc, dữ liệu từ đâu ra" |
| 5 | SQL warehouse | SQL Warehouse (Photon) | **Trino** | Query SQL nhanh, phục vụ BI và analyst |
| 6 | Transform + Orchestration | Workflows / DLT | **dbt-core + Airflow 3** | Biến dữ liệu thô thành bảng sạch, chạy đúng giờ đúng thứ tự |
| 7 | Consumption | Notebooks / Dashboards | **JupyterLab + Superset** | Nơi người dùng thật sự chạm vào dữ liệu |

Hai nhánh mở rộng:

| Nhánh | Databricks | Ta dùng | Vấn đề |
|---|---|---|---|
| Streaming | Structured Streaming + DLT | **Kafka (KRaft) + Spark Structured Streaming + Debezium** | Dữ liệu chảy liên tục thay vì chạy theo mẻ |
| ML | **MLflow** + Model Serving | **MLflow 3** *(chính hãng)* + feature table | Theo dõi thí nghiệm, quản lý phiên bản model, đưa model lên phục vụ |

### Vì sao lựa chọn này có sức nặng

Ba thứ ghi *(chính hãng)* — **Delta Lake, Unity Catalog, MLflow** — do chính Databricks viết ra
rồi mở mã nguồn. Học chúng là học **đúng thứ Databricks thật đang chạy**, không phải bản nhái
tương tự. Nghĩa là dự án này không phải "làm cái gì đó giống Databricks" mà là dựng lại
Databricks bằng chính ruột gan của nó.

### Các lựa chọn đã cân nhắc và loại

| Phương án | Quyết định | Lý do |
|---|---|---|
| Iceberg thay Delta | Delta làm chính | Mục tiêu là clone Databricks. Nhưng dành một buổi riêng ở phase sau để so sánh — "Delta khác Iceberg chỗ nào" là câu phỏng vấn hay gặp |
| Dagster thay Airflow | Airflow | Dagster thiết kế đẹp hơn, nhưng tin tuyển dụng hỏi Airflow nhiều gấp bội. Mục tiêu là xin việc |
| Hive Metastore thay Unity Catalog | Unity Catalog | Hive Metastore là đồ cũ, đang chết dần |

### Xác nhận tương thích arm64

Đã kiểm tra `docker manifest inspect` — **tất cả đều có arm64 native**, không phải emulate x86:

```
apache/spark:4.0.1                amd64 arm64
minio/minio:latest                amd64 arm64 ppc64le
trinodb/trino:476                 amd64 arm64 ppc64le
apache/airflow:3.1.0              amd64 arm64
apache/kafka:4.1.0                amd64 arm64
ghcr.io/mlflow/mlflow:v3.6.0      amd64 arm64
apache/superset:4.1.1             amd64 arm64
unitycatalog/unitycatalog:latest  amd64 arm64
quay.io/debezium/connect:3.0      amd64 arm64
postgres:17                       amd64 arm64 + nhiều kiến trúc khác
```

Lưu ý: tránh `bitnami/spark` — Bitnami đã thay đổi cách phát hành image công khai từ 2025.
Dùng `apache/spark` chính thức, hoặc tự build image từ nó khi cần thêm thư viện.

---

## Phần 2 — 12 phase, chia thành 4 chặng

| Chặng | Phase | Thời lượng | Kết thúc chặng có gì |
|---|---|---|---|
| **I. Nền móng** | 0-3 | ~11 tuần | Hiểu Spark + Delta, xử lý được dataset vài chục triệu dòng trên laptop |
| **II. Lakehouse hoàn chỉnh** | 4-7 | ~10 tuần | **Mốc CV** — platform batch đầy đủ, chạy tự động, có dashboard |
| **III. Tin cậy & thời gian thực** | 8-9 | ~6 tuần | Kiểm định chất lượng, lineage, pipeline streaming |
| **IV. ML & hoàn thiện** | 10-11 | ~6 tuần | MLflow, capstone, sẵn sàng phỏng vấn |

> **Mốc quan trọng: hết Phase 7 (~tháng thứ 5) là đủ đưa vào CV.**
> Ba phase cuối nâng từ "junior biết việc" lên "ứng viên nổi bật".
> Đừng chờ hết 9 tháng mới bắt đầu rải hồ sơ.

### Hai nguyên tắc xuyên suốt

1. **Cố ý làm sai trước, sửa sau.** Partition sai ở Phase 1, job chậm ở Phase 3, dữ liệu bẩn ở
   Phase 8. Người học từ tutorial trơn tru không trả lời được câu *"kể một lần pipeline của bạn
   hỏng"* — câu sàng lọc kinh điển.
2. **Đọc file thô, không chỉ gọi API.** Mở `_delta_log/`, đọc Parquet footer, xem Kafka offset.
   Hiểu tầng dưới là hiểu cả nhóm công nghệ, không chỉ một sản phẩm.

---

### Chặng I — Nền móng (Phase 0-3, ~11 tuần)

#### Phase 0 · Lát cắt mỏng — 2 tuần
- **Dựng:** `docker-compose.yml` đầu tiên: MinIO + JupyterLab. Cố tình dùng đồ đơn giản.
- **Làm:** Tải NYC Taxi dataset → đẩy lên MinIO → đọc bằng DuckDB → vẽ một biểu đồ.
- **Học:** Object storage là gì, S3 API, vì sao tách storage khỏi compute.
- **Xong khi:** `docker compose up` một phát ra pipeline chạy được; vẽ được sơ đồ 7 tầng lên
  giấy và chỉ đúng Phase 0 đang chiếm tầng nào.

#### Phase 1 · Storage & định dạng file — 2 tuần
- **Làm:** So sánh CSV / JSON / Parquet trên cùng dữ liệu — tự đo dung lượng và tốc độ query.
- **Học:** Columnar storage, nén, predicate pushdown, partition, row group.
- **Sai có chủ đích:** partition quá mịn để tự tay tạo ra "small files problem".
- **Xong khi:** giải thích được vì sao Parquet nhanh hơn CSV 10-50 lần **bằng số đo của mình**.

#### Phase 2 · Delta Lake — 3 tuần *(trái tim của Databricks)*
- **Học:** ACID transaction, time travel, `MERGE` (upsert), schema evolution, `OPTIMIZE`, `VACUUM`.
- **Trọng tâm:** mở `_delta_log/` đọc bằng tay — thấy transaction log chỉ là mấy file JSON.
- **Xong khi:** khôi phục được bảng về 3 phiên bản trước, và giải thích được Delta đạt ACID
  trên object storage **không có khoá** bằng cách nào.

#### Phase 3 · Apache Spark — 4 tuần *(phase nặng nhất)*
- **Học:** kiến trúc driver/executor, lazy evaluation, DataFrame API, và thứ quyết định tất cả:
  **shuffle**. Đọc Spark UI, xử lý data skew, broadcast join, repartition. Spark Connect từ Jupyter.
- **Sai có chủ đích:** viết một job chậm thảm hại rồi tự tối ưu.
- **Xong khi:** cầm job chậm → mở Spark UI → chỉ ra stage tốn thời gian và vì sao → tăng tốc ≥3 lần.

---

### Chặng II — Lakehouse hoàn chỉnh (Phase 4-7, ~10 tuần)

#### Phase 4 · Medallion + dbt — 3 tuần
Kiến trúc bronze/silver/gold. Chuyển logic transform từ script Python sang dbt models.
Thêm dbt tests và dbt docs. **Xong khi:** `dbt build` chạy sạch, có test bắt được lỗi thật.

#### Phase 5 · Trino + Superset — 2 tuần
Tầng SQL phục vụ BI, query liên kết nhiều nguồn, dashboard thật.
**Xong khi:** analyst tưởng tượng có thể tự trả lời câu hỏi mà không cần bạn.

#### Phase 6 · Unity Catalog — 2 tuần
Catalog ba tầng (catalog.schema.table), phân quyền, lineage. Phần governance mà **hầu hết người
tự học bỏ qua** — cũng chính là phần tạo khác biệt.

#### Phase 7 · Airflow 3 — 3 tuần
Trói tất cả thành DAG chạy tự động. Trọng tâm: **idempotency và backfill** — hai khái niệm
phỏng vấn DE nào cũng hỏi. **Xong khi:** chạy lại DAG của ngày hôm qua ra đúng kết quả cũ,
không nhân đôi dữ liệu.

---

### Chặng III — Tin cậy & thời gian thực (Phase 8-9, ~6 tuần)

#### Phase 8 · Chất lượng dữ liệu & observability — 2 tuần
Great Expectations + OpenLineage/Marquez. **Sai có chủ đích:** bơm dữ liệu bẩn vào và bắt hệ
thống tự chặn trước khi nó lan xuống gold.

#### Phase 9 · Streaming — 4 tuần
Kafka (KRaft, không Zookeeper), Spark Structured Streaming, checkpointing, watermark,
exactly-once semantics. Debezium bắt CDC từ Postgres đổ vào lakehouse.
**Xong khi:** giải thích được khác biệt giữa at-least-once và exactly-once bằng ví dụ trong
chính pipeline của mình.

---

### Chặng IV — ML & hoàn thiện (Phase 10-11, ~6 tuần)

#### Phase 10 · MLflow — 3 tuần
Tracking experiment, model registry, feature table trên Delta, batch inference chạy trong Airflow.

#### Phase 11 · Capstone — 3 tuần
Gộp tất cả thành một dự án **kể được thành câu chuyện**:
- README có sơ đồ kiến trúc
- `make up` chạy một phát
- CI trên GitHub Actions
- Một bài viết giải thích các quyết định thiết kế
- Luyện trả lời ~20 câu phỏng vấn rút ra từ chính dự án

---

## Theo dõi tiến độ

| Phase | Trạng thái | Ngày xong | Ghi chú |
|---|---|---|---|
| 0 · Lát cắt mỏng | ⬜ Chưa bắt đầu | | |
| 1 · Storage & file format | ⬜ | | |
| 2 · Delta Lake | ⬜ | | |
| 3 · Spark | ⬜ | | |
| 4 · Medallion + dbt | ⬜ | | |
| 5 · Trino + Superset | ⬜ | | |
| 6 · Unity Catalog | ⬜ | | |
| 7 · Airflow | ⬜ | | |
| 8 · Data quality | ⬜ | | |
| 9 · Streaming | ⬜ | | |
| 10 · MLflow | ⬜ | | |
| 11 · Capstone | ⬜ | | |

---

## Phần 3 — Phase 0 chi tiết: 12 bước để hiểu

> **Cách dùng:** đi từng bước một. Đọc giải thích → hiểu thì đánh dấu `[x]` → sang bước sau.
> Chưa hiểu thì hỏi lại, đừng đánh dấu. Mục tiêu là **hiểu**, không phải xong nhanh.

### Nhóm A — Chuẩn bị dự án

- [ ] **A1 · Cấu trúc thư mục** — vì sao tách `docker/`, `notebooks/`, `scripts/`, `data/`, `docs/`
- [ ] **A2 · File `.env`** — tách cấu hình khỏi code, vì sao không hardcode mật khẩu

### Nhóm B — Đọc hiểu `docker-compose.yml`

- [ ] **B1 · Service `minio`** — object storage, hai cổng 9000 vs 9001, `volumes`, `healthcheck`
- [ ] **B2 · Service `minio-init`** — job chạy một lần rồi thoát, `depends_on` có điều kiện
- [ ] **B3 · Service `jupyter`** — biến môi trường `AWS_*`, mount thư mục, `build` thay vì `image`
- [ ] **B4 · `volumes` và `networks`** — dữ liệu sống ở đâu, container gọi nhau bằng tên nào

### Nhóm C — Image tuỳ biến

- [ ] **C1 · Dockerfile + `requirements.txt`** — vì sao phải build image riêng thay vì dùng image có sẵn
- [ ] **C2 · Sự cố dependency thật** — version bịa và xung đột `s3fs`/`boto3`, cách chẩn đoán

### Nhóm D — Tiện ích vận hành

- [ ] **D1 · `Makefile` và `scripts/download_data.sh`** — vì sao cần lớp lệnh tắt

### Nhóm E — Luồng dữ liệu trong notebook

- [ ] **E1 · Nối MinIO và đẩy dữ liệu lên** — `boto3`, giao thức S3, cấu trúc key
- [ ] **E2 · DuckDB query thẳng từ `s3://`** — `SECRET`, `httpfs`, vì sao không cần `IMPORT`
- [ ] **E3 · Query nghiệp vụ và vẽ biểu đồ** — từ dữ liệu thô tới câu trả lời

### Nhóm F — Kiểm chứng

- [ ] **F1 · Cách kiểm chứng đúng** — `compose config`, build, chạy notebook bằng `nbclient`

