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
| 3 | Compute engine | Databricks Runtime | **Apache Spark 4.1** | Xử lý song song, tính toán trên dữ liệu lớn hơn RAM |
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
| Hive Metastore thay Unity Catalog | Unity Catalog | Hive Metastore là đồ cũ, đang chết dần. **Nhưng Phase 4-5 vẫn dùng nó làm đồ tạm** — chính việc tháo nó ra ở Phase 6 mới cho thấy Unity Catalog sinh ra để giải quyết gì. Xem Phần 7 |

### Xác nhận tương thích arm64

Đã pull thật về máy và kiểm chứng bằng `docker image inspect --format '{{.Architecture}}'`
(không chỉ đọc manifest). Trạng thái ngày 2026-08-24:

```
apache/spark:4.1.3                linux/arm64   ✅ đã pull
minio/minio:latest                linux/arm64   ✅ đã pull
minio/mc:latest                   linux/arm64   ✅ đã pull
postgres:17                       linux/arm64   ✅ đã pull
redis:7-alpine                    linux/arm64   ✅ đã pull
trinodb/trino:476                 linux/arm64   ✅ đã pull
apache/airflow:3.1.0              linux/arm64   ✅ đã pull
apache/superset:4.1.1             linux/arm64   ✅ đã pull
unitycatalog/unitycatalog:v0.6.0  linux/arm64   ✅ đã pull
ghcr.io/mlflow/mlflow:v3.6.0      linux/arm64   ✅ đã pull
apache/kafka:4.1.0                linux/arm64   ✅ đã pull
quay.io/debezium/connect:3.0      linux/arm64   ✅ đã pull
marquezproject/marquez:latest     linux/amd64   ⚠️ đã pull — CHỈ amd64
marquezproject/marquez-web:latest linux/amd64   ⚠️ đã pull — CHỈ amd64
apache/hive:4.0.0                 linux/arm64   ⚠️ manifest đa kiến trúc có arm64,
                                                    chưa `docker image inspect` trên
                                                    máy arm64 thật (dựng Phase 5 trên
                                                    máy phát triển x86_64)
```

**Hai ngoại lệ cần biết trước khi tới Phase 8:** Marquez (backend OpenLineage) chưa phát hành
image arm64. Trên Apple Silicon nó sẽ chạy qua emulation x86 — khởi động chậm hơn và tốn RAM
hơn. Ba lựa chọn khi tới phase đó: chấp nhận emulation, tự build image arm64 từ source, hoặc
thay bằng phương án lineage khác. Quyết định sau, khi đã thấy nó chậm tới mức nào.

**Về Unity Catalog:** dùng tag cố định `v0.6.0` chứ không phải `latest`. Tag `latest` trỏ tới
một image khác (image ID khác hẳn) và có thể đổi bất cứ lúc nào — pin version giữ cho môi
trường tái lập được.

**Về dung lượng đĩa:** toàn bộ image của lộ trình chiếm khoảng **25GB**. Cộng thêm build cache
Docker (dễ phình lên 20GB+ sau vài lần build) và volume dữ liệu. Chừa sẵn ~40GB trống, và
chạy `docker builder prune -f` khi thấy chật.

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

> ✅ Đã xong. `dbt build` cho **21/21 node PASS** (5 model + 1 seed + 16 test), và ba
> bài test đã bắt được lỗi thật ngay
> lần chạy đầu (4 dòng trùng, 107.092 dòng mồ côi, thiếu giá trị borough hợp lệ).
> Chi tiết ở [Phần 7](#phần-7--phase-4-chi-tiết-medallion--dbt).

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
| 0 · Lát cắt mỏng | ✅ Xong | 2026-08-23 | `notebooks/00_thin_slice.ipynb` |
| 1 · Storage & file format | ✅ Xong | 2026-08-23 | `notebooks/01_file_formats.ipynb` |
| 2 · Delta Lake | ✅ Xong | 2026-08-23 | `notebooks/02_delta_lake.ipynb` |
| 3 · Spark | ✅ Xong | 2026-08-23 | `notebooks/03_spark.ipynb` |
| 4 · Medallion + dbt | ✅ Xong | 2026-08-24 | `notebooks/04_medallion_dbt.ipynb` + `dbt/` |
| 5 · Trino + Superset | ✅ Xong | 2026-08-24 | `notebooks/05_trino_superset.ipynb` + Hive Metastore độc lập |
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

- [x] **A1 · Cấu trúc thư mục** — vì sao tách `docker/`, `notebooks/`, `scripts/`, `data/`, `docs/`
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

---

## Phần 4 — Phase 1 chi tiết: đo bằng tay, không tin lời đồn

> **Sổ tay:** `notebooks/01_file_formats.ipynb`. Chạy từng cell, **nhìn số của chính mình
> trước**, đọc giải thích sau. Không cần cài thêm gì — vẫn MinIO + Jupyter của Phase 0.

Phase 1 không thêm service nào vào `docker-compose.yml`. Đó là chủ ý: phase này đào
**sâu xuống tầng 1**, chứ không mở rộng ra ngang. Thứ thay đổi là hiểu biết, không phải stack.

### Sáu bước

- [x] **1 · Năm cách lưu cùng một dữ liệu** — CSV / JSON / Parquet (không nén, Snappy, Zstd).
      Đo byte. Hiểu vì sao Parquet *không nén* đã nhỏ hơn CSV.
- [x] **2 · Bấm giờ ba truy vấn** — `count(*)`, một cột, lọc+nhóm. Hiểu **projection pushdown**
      và vì sao `count(*)` trên Parquet gần như tức thì.
- [x] **3 · Mổ bụng file Parquet** — đọc footer bằng `pyarrow`: row group, column chunk,
      encoding, thống kê min/max. *Nguyên tắc "đọc file thô" của lộ trình.*
- [x] **4 · Thứ tự dòng quyết định tốc độ** — cùng dữ liệu, cùng dung lượng: bản xáo trộn
      phải đọc 25/25 row group, bản sắp xếp chỉ đọc 2/25. **Đây chính là Z-ORDER.**
- [x] **5 · Partition Hive-style** — `ngay=2024-01-15/` trên tên thư mục. Đọc `EXPLAIN` thấy
      `Scanning Files: 1/31` — **partition pruning**. Ba tiêu chí chọn cột partition.
- [x] **6 · SAI CÓ CHỦ ĐÍCH: small files problem** — partition theo ngày×giờ, đẻ ra hơn
      nghìn file nhỏ, chậm hơn cả chục lần trong khi dung lượng còn *phình ra*. Rồi tự tay
      compaction để chữa — chính là thứ Delta gọi là `OPTIMIZE`.

### Ba câu phải trả lời được trước khi sang Phase 2

1. Parquet nhanh hơn CSV nhờ **ba** cơ chế nào? (Nén **không** nằm trong ba cái đó.)
2. Hai file Parquet cùng dữ liệu, cùng dung lượng, một cái query nhanh hơn 10 lần — khác ở đâu?
3. Tổng dung lượng gần như không đổi, sao nghìn file nhỏ lại chậm hơn 31 file? Tiền và
   thời gian trên object storage tính theo cái gì?

### Cầu nối sang Phase 2

Ở bước 6 bạn gộp file bằng tay. Nếu lúc đó có người đang query, **họ sẽ thấy dữ liệu hỏng**:
file cũ đã xoá, file mới chưa ghi xong. Một thư mục Parquet trần không có khái niệm
"phiên bản" hay "giao dịch".

Đó đúng là lỗ hổng Delta Lake sinh ra để vá — và là toàn bộ nội dung Phase 2.

---

## Phần 5 — Phase 2 chi tiết: Delta Lake

> **Sổ tay:** `notebooks/02_delta_lake.ipynb`. Cần build lại image một lần
> (`make up` — đã thêm `deltalake==1.6.3` vào `docker/jupyter/requirements.txt`).

### Vì sao `delta-rs` chứ không phải `delta-spark`

Spark là nội dung Phase 3. Kéo nó vào đây sẽ trộn hai bài học lớn làm một, và bạn sẽ
không phân biệt nổi đâu là *Delta* đâu là *Spark*. `delta-rs` là thư viện Rust cài đặt
**đúng Delta protocol chuẩn** — `_delta_log/` sinh ra giống hệt thứ Databricks thật ghi,
không cần JVM, khởi động tức thì.

Đến Phase 3, Spark sẽ mở đúng bảng bạn tạo hôm nay mà không cần export/import gì.
Đó là bằng chứng sống cho luận điểm "Delta là định dạng mở".

### Chín bước

- [x] **1 · Bảng Delta đầu tiên** — nhìn ra storage: chỉ có file Parquet + thư mục `_delta_log/`.
      **Một bảng Delta chỉ là một thư mục.**
- [x] **2 · Mở `_delta_log/` đọc bằng tay** ⭐ *trọng tâm cả phase* — ba action `protocol` /
      `metaData` / `add`. Hiểu: **bảng = tập file mà log nói là thuộc về nó**, không phải
      mọi file trong thư mục. Thống kê min/max được chép sẵn lên log → data skipping ở tầng bảng.
- [x] **3 · ACID không cần khoá** — tự tay thử `put-if-absent` (`If-None-Match: *`) trên MinIO:
      writer thứ hai bị `PreconditionFailed`. **Đó là toàn bộ cơ chế khoá của Delta.**
      Kèm demo snapshot isolation.
- [x] **4 · Time travel + `RESTORE`** — và vì sao restore không xoá lịch sử (log append-only).
- [x] **5 · `MERGE` (upsert)** — sửa 2 dòng nhưng `num_target_rows_copied` = 499.998.
      Hiểu **copy-on-write** và lối thoát *deletion vectors* (merge-on-read).
- [x] **6 · Schema evolution** — Delta chặn trước, chỉ cho qua khi khai `schema_mode='merge'`.
      Thêm cột vào bảng 10 TB tốn vài giây vì **schema nằm trong log, không nằm trong file**.
- [x] **7 · `OPTIMIZE` + `Z-ORDER`** — chính việc đã làm tay ở Phase 1, nay gói trong transaction.
      Bảng min/max sau Z-ORDER cho thấy mỗi file chỉ ôm một dải `pu_zone` hẹp.
- [x] **8 · SAI CÓ CHỦ ĐÍCH: `VACUUM` nuốt time travel** — vacuum retention 0 rồi đọc version
      cũ → `FileNotFoundError`. Log còn, dữ liệu mất. Đây là sự cố production rất phổ biến.
- [x] **9 · Bằng chứng định dạng mở** — ghi bằng `delta-rs`, đọc bằng `delta_scan` của DuckDB.

### Sáu câu phải trả lời được trước khi sang Phase 3

1. S3 không có khoá — hai job cùng ghi thì ai thắng, kẻ thua làm gì tiếp?
2. Vì sao Databricks từng phải dùng DynamoDB cho Delta trên S3, còn nay thì không?
3. Thêm một cột vào bảng 10 TB mất bao lâu? Vì sao?
4. `MERGE` sửa 2 dòng nhưng ghi lại nửa triệu dòng — vì sao? *Deletion vectors* đổi gì?
5. `VACUUM` retention 1 giờ trong khi có job Spark chạy 3 tiếng — chuyện gì xảy ra?
6. `RESTORE` về version 1 có xoá version 2 không, và vì sao câu trả lời quan trọng?

### Cầu nối sang Phase 3

Delta đã vá xong ba lỗ hổng của Phase 1 (giao dịch, phiên bản, upsert). Nhưng mọi thứ
trong notebook này vẫn chạy **trên một máy, dữ liệu vừa RAM**.

Câu hỏi còn lại: dữ liệu 500 GB thì tính bằng gì? Đó là Phase 3 — Apache Spark,
phase nặng nhất của cả lộ trình.


---

## Phần 6 — Phase 3 chi tiết: Apache Spark

> **Sổ tay:** `notebooks/03_spark.ipynb`. Cần `make up` để build image Spark
> (lần đầu ~10 phút, chủ yếu là 611MB AWS SDK) và `make data` để tải đủ 12 tháng.

Đây là phase đầu tiên kể từ Phase 0 **thêm service mới vào stack** — và thêm một lúc bốn cái.
Phase 1 và 2 đào sâu xuống tầng dưới; Phase 3 dựng hẳn tầng 3 lên trên.

### Bốn container, một image

```
jupyter ──sc://15002──► spark-connect (DRIVER) ──7077──► spark-master
  client mỏng                 │                              │
                          UI :4040                    ┌──────┴──────┐
                                                  worker-1      worker-2
                                                  2 core/4GB    2 core/4GB
```

Cả bốn dùng **chung một image** (`docker/spark/Dockerfile`). Vai trò khác nhau hoàn toàn do
`command`. Đó không phải mẹo tiết kiệm — Spark thật cũng vậy: cùng một bộ binary, khác nhau
ở tiến trình nào được khởi động.

**Điểm dễ hiểu nhầm nhất:** notebook của bạn **không chứa driver**. Driver sống trong container
`spark-connect`, thường trực, nhận lệnh qua gRPC. Notebook chỉ là bàn phím. Đây đúng là mô hình
**Databricks Connect** — và là lý do `docker/jupyter/requirements.txt` cài `pyspark-client`
chứ không phải `pyspark`.

**Vì sao hai worker chứ không một:** để shuffle buộc phải đi qua network giữa hai JVM khác nhau.
Một worker thì shuffle chỉ là chép file trong cùng tiến trình — nhìn không ra vấn đề.
Tổng 4 core là con số cố ý nhỏ: đủ để một task lệch (skew) treo trong khi ba task kia đã rảnh.

### Ba cái bẫy version đã gỡ sẵn

Ghi lại vì đây đúng là kiểu lỗi mất cả buổi tối, và là bài học C2 của Phase 0 lặp lại ở quy mô lớn hơn:

| Bẫy | Sai ở đâu | Đúng là |
|---|---|---|
| `delta-spark` bản mới nhất | 4.4.0 khai `spark-sql 4.2.0` trong pom | **4.3.1** — khai `4.1.0`, khớp image |
| AWS SDK | Hướng dẫn cũ dạy `aws-java-sdk-bundle` (SDK v1) | Hadoop 3.4 dùng **SDK v2**: `software.amazon.awssdk:bundle` |
| `hadoop-aws` | Chọn đại bản mới | Phải khớp **chính xác** `hadoop-client-*.jar` trong image (3.4.2) |
| Python UDF chết ngay lần gọi đầu | Image Spark mang Python 3.10, notebook chạy 3.13 | Cài thêm Python **3.13** vào image Spark, trỏ `PYSPARK_PYTHON` vào đó |
| `Permission denied` khi chạy UDF | `uv` cài Python vào `/root` (mode 700); executor chạy bằng user `spark` | `UV_PYTHON_INSTALL_DIR=/opt/...` + `chmod a+rX` |
| `Missing an output location for shuffle` | Docker Desktop chỉ cấp ~8GB dù máy 32GB. Executor bị kernel giết (`exit 137`) | Tổng RAM Spark phải **dưới trần Docker VM**: 2×2g + driver 1g |

"Mới nhất" và "đúng" là hai chuyện khác nhau. Cách tra: đọc `pom` trên Maven Central, không đoán.

Bẫy cuối đáng nhớ vì lý do khác: **lỗi hiện ra ở tầng shuffle, nguyên nhân nằm ở tầng hệ
điều hành**. Spark báo "thiếu output của shuffle", nhưng sự thật là executor đã bị kernel
giết vì hết RAM. Gặp `MetadataFetchFailedException`, việc đầu tiên nên làm là
`docker compose logs spark-worker-1 | grep 137` chứ không phải chỉnh `spark.sql.shuffle.partitions`.

Cái bẫy Python đáng nhớ nhất: UDF được **pickle ở client, unpickle ở executor**. Khác version
là khác định dạng bytecode. Databricks cũng bắt version Databricks Connect phải khớp Databricks
Runtime — cùng một lý do, chỉ là họ giấu nó sau bảng tương thích trên tài liệu.

### Chín bước

- [ ] **1 · Nối cluster bằng `sc://`** — in ra executor thật. Mở song song :8080 và :4040,
      đối chiếu với sơ đồ trên. Hiểu driver ở đâu, executor ở đâu, ai nói chuyện với ai.
- [ ] **2 · Lazy evaluation** — transformation không sinh job nào, action mới sinh.
      Đếm job trên UI để **tự chứng minh**, không tin lời sách.
- [ ] **3 · Partition sinh ra từ đâu** — đọc 12 tháng từ MinIO qua S3A, `getNumPartitions()`.
      Số file × kích thước split, không phải con số ngẫu nhiên.
- [ ] **4 · Narrow vs wide** ⭐ — `filter`/`select` không shuffle, `groupBy` thì có.
      Nhìn DAG trên UI: **ranh giới stage chính là shuffle**. Đây là ý niệm quan trọng nhất
      của cả phase — mọi thứ về hiệu năng Spark đều quy về câu hỏi "có shuffle không, bao nhiêu".
- [ ] **5 · SAI CÓ CHỦ ĐÍCH ①: job chậm thảm hại** — Python UDF + `repartition` thừa +
      join không broadcast. Rồi chữa từng thứ một, đo lại sau mỗi lần. **Mục tiêu ≥3×.**
- [ ] **6 · Broadcast join vs sort-merge** — `explain()` thấy plan đổi hẳn chiến lược,
      và thấy broadcast xoá được **cả một shuffle** chứ không chỉ nhanh hơn chút ít.
- [ ] **7 · SAI CÓ CHỦ ĐÍCH ②: data skew** — cố ý làm key lệch, thấy 1 task chạy mãi trong khi
      3 task kia xong từ lâu. Chữa bằng salting, rồi tắt/bật AQE xem Spark 4 tự xử đến đâu.
- [ ] **8 · Spark mở đúng bảng Delta của Phase 2** — không export, không convert, không import.
      Rồi ghi ngược lại bằng Spark cho `delta-rs` đọc. Bằng chứng sống cho
      "Delta là định dạng mở" mà Phần 5 đã hứa.
- [ ] **9 · Khi nào KHÔNG nên dùng Spark** — ở quy mô 36 triệu dòng, DuckDB một máy vẫn thắng.
      Biết giới hạn này là câu trả lời phỏng vấn tốt hơn nhiều so với ca ngợi Spark.

### Năm câu phải trả lời được trước khi sang Phase 4

1. Driver và executor, cái nào chạy code trong cell notebook của bạn? Cái nào giữ kết quả `collect()`?
2. Vì sao `df.filter(...)` chạy tức thì còn `df.filter(...).count()` mất 30 giây?
3. Shuffle tốn ở chỗ nào — đĩa, network, hay CPU? Kể tên hai cách xoá bỏ một shuffle.
4. Một stage có 200 task, 199 task xong trong 2 giây, 1 task chạy 4 phút. Chuyện gì đang xảy ra
   và bạn sửa thế nào?
5. Dữ liệu 5GB, một máy 32GB RAM — vì sao Spark có thể **chậm hơn** DuckDB?

### Cầu nối sang Phase 4

Hết Phase 3, bạn có đủ ba mảnh của một lakehouse: storage (MinIO), table format (Delta),
compute (Spark). Nhưng logic biến đổi dữ liệu vẫn nằm rải rác trong các cell notebook —
không ai chạy lại được, không ai test được, không ai biết bảng nào phụ thuộc bảng nào.

Đó là vấn đề Phase 4 giải: **medallion + dbt**.

---

## Phần 7 — Phase 4 chi tiết: Medallion + dbt

> **Sổ tay:** `notebooks/04_medallion_dbt.ipynb`. Cần `make up` (build image dbt, ~1 phút)
> rồi `make ingest` và `make dbt`.

Phase 3 dựng tầng compute. Phase 4 dựng **tầng 6 — transform** và, lặng lẽ hơn nhưng
quan trọng không kém, dựng luôn một mẩu **tầng 4 — catalog**.

### Vì sao phải rời khỏi notebook

Thử hỏi ba câu này về `03_spark.ipynb`: bảng `gold_borough` tính từ đâu qua mấy bước?
Đổi bộ lọc ở giữa thì phải dựng lại những bảng nào? Làm sao biết kết quả hôm nay đúng?

Không câu nào trả lời được. Đó là **giới hạn của notebook như công cụ sản xuất**: thứ
tự chạy phụ thuộc người bấm chuột, không ai test được, không ai biết cái gì phụ thuộc
cái gì. dbt giải đúng ba chuyện đó, và không giải gì khác.

### Kiến trúc medallion — và ranh giới của dbt

```
raw/            bronze              silver               gold
────────        ──────────────      ────────────────     ──────────────
Parquet    →    nguyên trạng   →    sạch, đúng kiểu  →   trả lời câu hỏi
của TLC         + 3 cột `_`         1 dòng = 1 chuyến    đã tổng hợp

41.169.720      41.169.720          35.613.229           80.523 + 1.300

script ingest   ────────── dbt lo từ đây trở đi ──────────────►
```

**dbt không nạp dữ liệu.** Chữ T trong ELT — Transform. Việc kéo file từ ngoài vào là
E/L, thuộc về `scripts/ingest_bronze.py` (đời thật: Fivetran, Airbyte, Auto Loader).
Trộn hai việc vào dbt là lỗi kiến trúc thường gặp, và nó phá luôn khả năng test:
dbt không test nổi thứ nó tự nạp.

Quy tắc mỗi tầng, gọn lại thành một dòng:

| Tầng | Được làm gì | Cấm làm gì |
|---|---|---|
| bronze | chép nguyên trạng, thêm cột `_` | đổi tên cột, ép kiểu, lọc dòng |
| silver | đổi tên, ép kiểu, **lọc** dòng vô lý | tổng hợp, **sửa** giá trị |
| gold | tổng hợp theo câu hỏi nghiệp vụ | giữ độ mịn từng dòng |

Silver **LỌC**, không **SỬA**. Đoán xem một chuyến giá âm "đáng lẽ" là bao nhiêu chính
là bịa dữ liệu. Dòng bị loại vẫn nằm nguyên ở bronze.

### Hai quyết định kiến trúc của phase này

**1. dbt nối vào Spark bằng `method: session`, không dựng Thrift Server.**

dbt-spark có bốn cách kết nối: `thrift`, `http`, `odbc`, `session`. Ba cách đầu đều cần
dựng thêm một Spark Thrift Server. Không cần — Spark Connect đã là cái cổng đó rồi.
Vì image dbt cài `pyspark-client` và có `SPARK_REMOTE`, "session" thực chất là một
session **từ xa**. Kết quả: dbt và notebook dùng **chung một driver, chung một catalog**.
Bảng dbt tạo, notebook thấy ngay.

Điểm đáng nhớ cho phỏng vấn: Databricks dùng `dbt-databricks`, và nó chính là một
**nhánh rẽ của `dbt-spark`**. Học cái này là học đúng cái kia.

**2. Catalog tạm bằng Hive Metastore nhúng — và biết rõ nó tạm.**

Từ Phase 4, `spark-defaults.conf` bật `spark.sql.catalogImplementation hive`, ghi vào
Derby trên volume. Vì sao phải có: model incremental cần hỏi *"bảng này đã tồn tại
chưa?"*, mà catalog mặc định `in-memory` thì danh sách bảng bốc hơi mỗi lần
`spark-connect` khởi động lại.

Đây mâu thuẫn có chủ đích với bảng "các lựa chọn đã loại" ở Phần 1. Hive Metastore
đúng là đồ cũ, và chính vì thế nó ở đây: **Phase 6 sẽ tháo nó ra thay bằng Unity
Catalog**, và lúc tháo mới thấy rõ UC sinh ra để giải quyết gì. Đọc bảng so sánh thì
quên ngay; tự tay gỡ một thứ vì nó không đủ dùng thì nhớ mãi.

### Tám bước

- [x] **1 · Bảng có TÊN thay vì đường dẫn** — `show databases`, `describe extended`.
      Managed và external khác nhau ở đúng một chỗ: `DROP TABLE` có xoá file hay không.
- [x] **2 · Bronze và tổng điều tra chất lượng** — đếm rác TRƯỚC khi viết bộ lọc.
      **13,5% dữ liệu là rác**, riêng `passenger_count = 0` đã chiếm gần 11%.
- [x] **3 · Một model dbt thực chất là gì** — mở `target/compiled/` và `target/run/`
      đọc SQL thật. Gỡ lỗi dbt là **đọc file, không phải đoán**.
- [x] **4 · Chạy pipeline, đo cái phễu** — 41,1 triệu → 35,6 triệu → 80 nghìn dòng.
      Kèm một cái bẫy nghiệp vụ: tỷ lệ boa chỉ đúng khi lọc riêng chuyến trả thẻ.
- [x] **5 · Test — biến "chạy được" thành "tin được"** ⭐ — bốn test dựng sẵn + singular
      test đối chiếu tổng. Rồi tự tay làm một bài test đỏ bằng `--vars`.
- [x] **6 · SAI CÓ CHỦ ĐÍCH: pipeline không idempotent** ⭐ — `append` thay vì `merge`.
      Chạy ba lần, dữ liệu ×3, mà dbt báo *"Completed successfully"* cả ba lần.
- [x] **7 · Lineage** — sơ đồ không ai vẽ, dbt suy từ `ref()`. Và `state:modified+`,
      thứ khiến CI chạy 2 phút thay vì 2 tiếng.
- [x] **8 · dbt KHÔNG phải một engine** — container dbt không có JVM, không đọc byte nào
      từ MinIO. Mở :4040 thấy job Spark do dbt sinh ra. Song song với bài học
      "notebook không chứa driver" của Phase 3.

### Bốn cái bẫy đã sập thật khi dựng phase này

Ghi lại vì cả bốn đều mất thời gian thật, và cả bốn đều còn nguyên trong mã dưới dạng
chú thích tại đúng chỗ gây ra chúng.

| Bẫy | Triệu chứng | Nguyên nhân thật |
|---|---|---|
| Kiểu `INTERVAL` | `DELTA_UNSUPPORTED_DATA_TYPES` | `dropoff - pickup` ra kiểu INTERVAL, Delta không lưu được. Phải quy về số |
| Dấu nháy trong seed | `PARSE_SYNTAX_ERROR at or near 's'` | "Governor's Island". `method: session` nội suy chuỗi trần, không thoát ký tự — **bug của adapter**, vá bằng macro trong dự án |
| Lọc bảng chiều | test `relationships` đỏ **107.092 dòng** | `where Borough != 'Unknown'` xoá mất vùng 264. Bảng chiều phải ĐẦY ĐỦ |
| `DROP TABLE` external | seed 265 dòng thành **530** | Drop bảng external không xoá file; create ngay sau đó nhận vơ dữ liệu cũ rồi chèn thêm |

Cái bẫy thứ hai đáng nhớ nhất, không phải vì khó mà vì bài học đi kèm: **adapter cũng
có bug, và dbt cho bạn vá nó ngay trong dự án** — khai lại đúng tên macro trong
`macros/` là ghi đè được, không cần fork, không cần chờ bản vá thượng nguồn.

### Ba bài test đã bắt được lỗi thật

Không phải test cho có. Ba trong số 16 bài test đỏ ngay lần chạy đầu:

1. `unique` trên `trip_id` → **4 dòng trùng hoàn toàn** trong 36 triệu dòng dữ liệu
   gốc của TLC. Tỷ lệ chẳng ảnh hưởng doanh thu, nhưng đủ **làm chết job hàng đêm**:
   MERGE đòi khoá duy nhất, một khoá khớp hai dòng nguồn là Delta ném lỗi.
2. `relationships` → 107.092 dòng mồ côi (xem bảng bẫy ở trên).
3. `accepted_values` trên `borough` → thiếu `Unknown` và `N/A`. Bài test này kiểm chứng
   **hiểu biết của ta về dữ liệu** nhiều hơn là kiểm chứng dữ liệu.

### Idempotency — ba cơ chế, ba tầng

Đây là ý niệm quan trọng nhất Phase 4 để lại, và là cầu nối thẳng sang Phase 7.

| Tầng | Cơ chế | Ở đâu |
|---|---|---|
| ingest | `replaceWhere` theo tháng | `scripts/ingest_bronze.py` |
| silver | `MERGE` trên khoá duy nhất | `config(unique_key='trip_id')` |
| gold | dựng lại toàn bộ | `+materialized: table` |

Tầng gold dùng cách thô nhất — xoá đi dựng lại — và **đó là lựa chọn đúng**: 80 nghìn
dòng dựng lại mất 15 giây, đổi lại là không bao giờ phải nghĩ về idempotency ở tầng đó
nữa. Tối ưu sớm ở chỗ không đáng là một dạng nợ kỹ thuật.

### Năm câu phải trả lời được trước khi sang Phase 5

1. Vì sao bronze **không được** đổi tên cột cho đẹp?
2. `dbt run` và `dbt build` khác nhau ở đâu, vì sao nên luôn dùng cái sau?
3. Model A dùng `merge`, model B dùng `append`. Chạy pipeline hai lần, chuyện gì xảy ra
   với từng cái — và cái nào báo lỗi cho bạn biết?
4. `DROP TABLE` một bảng managed và một bảng external khác nhau thế nào?
5. Muốn dbt chạy nhanh hơn thì sửa ở đâu? *(Bẫy: câu trả lời không nằm trong dbt.)*

### Cầu nối sang Phase 5

`gold_daily_zone_revenue` có 80.523 dòng — nhỏ, sạch, sẵn sàng cho dashboard. Nhưng để
nhìn thấy nó, hiện vẫn phải mở notebook và biết viết PySpark. Không analyst nào làm vậy.

Phase 5 dựng cổng **SQL thuần** cho họ: Trino + Superset.

Và ở đó sẽ lộ ra đúng giới hạn của cái catalog tạm dựng hôm nay: **Hive Metastore nhúng
bằng Derby chỉ một tiến trình JVM mở được**. Trino sẽ không nhìn thấy bảng nào cả. Đó
chính là lúc câu hỏi *"vì sao cần Unity Catalog"* trở thành câu hỏi của bạn, chứ không
còn là một dòng trong bảng so sánh ở Phần 1.

---

## Phần 8 — Phase 5 chi tiết: Trino + Superset

> **Sổ tay:** `notebooks/05_trino_superset.ipynb`. Cần `make up` (build thêm
> `hive-metastore`, `postgres`, `trino`, `superset`, ~2 phút). Vì catalog đổi từ Derby
> nhúng sang Hive Metastore độc lập, lượt dựng phase này đi kèm một lần rebuild sạch:
> `make clean && make up && make data && make ingest && make dbt`.

### Quyết định kiến trúc: không vá Trino, mà thay cả catalog

Dự định ban đầu là "chỉ thêm Trino". Nhưng thử trỏ Trino vào Derby nhúng của Phase 4
thì lộ ngay: đó không phải một service mạng, mà là một *thư viện chạy trong đúng JVM
của `spark-connect`* — không có cổng Thrift nào để Trino gõ cửa. Vá riêng cho Trino
(vd: một bản sao catalog khác) sẽ tạo ra HAI nguồn sự thật về "bảng nào ở đâu" — đúng
kiểu nợ kỹ thuật dự án này cố tránh từ đầu.

Cách sửa đúng: tách catalog thành một service độc lập mà **cả Spark lẫn Trino cùng
trỏ vào**.

```
jupyter/dbt ──sc://15002──► spark-connect ──┐
                                             ├──► hive-metastore ──thrift:9083──► postgres
trino ───────────HTTP :8080─────────────────┘         (metastore_db)

superset ──sqlalchemy-trino──► trino
```

- **`hive-metastore`** — `apache/hive:4.0.0` chạy vai `metastore` độc lập
  (`SERVICE_NAME=metastore`), ghi vào Postgres qua JDBC thay vì Derby. Đây KHÔNG phải
  Unity Catalog — không phân quyền, không lineage, không đa catalog — nhưng đã là một
  **service thật**, điều Derby nhúng chưa từng là.
- **`postgres`** — dùng chung một container cho cả metadata Hive Metastore lẫn metadata
  Superset (hai database khác nhau), thay vì dựng hai Postgres chỉ để tiết kiệm RAM.
- **`trino`** — catalog duy nhất `delta`, connector **`delta_lake`** chứ không phải
  `hive`: đọc thẳng `_delta_log/`, chỉ mượn Hive Metastore để biết đường dẫn bảng. Tên
  file `docker/trino/etc/catalog/delta.properties` chính là tên catalog — nguồn gốc của
  cú pháp `delta.silver.silver_trips` ba tầng.
- **`superset`** — ảnh gốc build thêm driver `trino[sqlalchemy]` + `psycopg2-binary`.

`spark-defaults.conf` đổi từ

```
spark.hadoop.javax.jdo.option.ConnectionURL  jdbc:derby:;databaseName=...
```

sang một dòng:

```
spark.hadoop.hive.metastore.uris  thrift://hive-metastore:9083
```

### Sáu bước

- [x] **1 · `catalog.schema.table`** — `SHOW CATALOGS`, `SHOW SCHEMAS FROM delta`. Ba
      tầng tên chứ không hai — và tầng thêm vào chính là bản nháp của Unity Catalog.
- [x] **2 · Cùng một `_delta_log/`, ba engine đọc** — đối chiếu số Trino ra với số Spark/
      dbt đã đo ở Phase 4. Khớp tuyệt đối: 41.169.720 → 35.613.229 → 80.523 + 1.300.
- [x] **3 · SAI CÓ CHỦ ĐÍCH: catalog nhúng không phải dịch vụ dùng chung** ⭐ — trỏ
      `hive.metastore.uri` của Trino vào một cổng không ai lắng nghe, thấy Trino vẫn
      *khởi động khoẻ mạnh* nhưng `SHOW SCHEMAS` lỗi thật. Rồi sửa bằng kiến trúc catalog
      độc lập ở trên.
- [x] **4 · Trả lời câu hỏi nghiệp vụ bằng SQL thuần** — doanh thu theo borough, nhu cầu
      theo giờ — không một dòng PySpark, không mở `spark-connect`.
- [x] **5 · Superset: SQL Lab → dataset → dashboard** — nối `Trino Lakehouse` qua
      `trino://trino@trino:8080/delta`, dựng dashboard **"Lakehouse — NYC Taxi 2024"**
      với 2 chart thật (doanh thu theo ngày, nhu cầu theo giờ).
- [x] **6 · dbt KHÔNG phải một engine, Trino cũng KHÔNG PHẢI SPARK** — mở `:4040` lúc
      chạy query Trino: không job nào xuất hiện. Song song bài học Phase 4 về dbt.

### Bốn cái bẫy đã sập thật khi dựng phase này

| Bẫy | Triệu chứng | Nguyên nhân thật |
|---|---|---|
| Tên thuộc tính S3 của Trino | `Configuration property 'fs.s3.enabled' was not used` | Trino 476 dùng `fs.native-s3.enabled`, không phải `fs.s3.enabled` — tài liệu/bài viết cũ trên mạng còn dạy tên cũ |
| Hive Metastore không đọc nổi `s3a://` | `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` | Chính Metastore (không chỉ Spark) tự validate LOCATION bảng — cần `hadoop-aws` khớp bản Hadoop **3.3.6** ảnh `apache/hive` mang theo, tức SDK v1 (`aws-java-sdk-bundle:1.12.367`), khác hẳn cặp Hadoop 3.4.2 / SDK v2 của ảnh Spark |
| `docker-compose.yml` — entrypoint nhiều dòng vỡ | `superset fab create-admin` chạy không cờ, mỗi `--flag` thành một LỆNH SHELL riêng (`/bin/sh: --username: not found`) | YAML folded scalar (`>`) chỉ gộp dòng thành dấu cách khi MỌI dòng thẳng hàng; dòng thụt sâu hơn (các `--flag`) bị giữ nguyên xuống dòng. Sửa bằng entrypoint dạng list + một chuỗi `>-` phẳng, không dòng nào thụt lệch |
| Dashboard Superset dựng qua API "trống trơn" | Chart đã gắn vào dashboard (`ADDED`) nhưng mở lên không thấy gì | `position_json` tự viết tay thiếu đúng thuộc tính kích thước grid mà Superset cần — chart tồn tại nhưng cao 0px. Sửa bằng kéo-thả thật trong UI thay vì tự sinh JSON layout |

Cái bẫy thứ hai đáng nhớ nhất: đây là LẦN THỨ HAI trong dự án một thành phần cần
`hadoop-aws` khớp chính xác phiên bản Hadoop nó mang theo (lần đầu là
`docker/spark/Dockerfile` ở Phase 3) — nhưng lần này là một thành phần hoàn toàn khác
(Hive Metastore, không phải Spark), với một cặp version khác hẳn. Bài học không phải
"nhớ đúng một con số", mà là **luôn tra `hadoop-*` bundled trong chính ảnh trước khi
chọn `hadoop-aws`**, không suy diễn từ thành phần khác.

### Năm câu phải trả lời được trước khi sang Phase 6

1. Trino đọc bảng Delta bằng connector nào — `hive` hay `delta_lake`? Khác nhau ở đâu?
2. Vì sao container Trino có thể "healthy" mà một câu `SHOW SCHEMAS` vẫn lỗi?
3. `catalog.schema.table` của Trino ánh xạ vào đâu trong Hive Metastore — có thật sự
   tồn tại một tầng "catalog" ở đó không?
4. Metastore nhúng (Derby) và metastore độc lập (Postgres + Thrift) khác nhau ở ĐÚNG
   một chỗ nào khiến Trino dùng được cái này mà không dùng được cái kia?
5. Superset lưu dashboard/chart ở đâu — trong Trino, hay một chỗ khác? Vì sao phải tách
   hai cái đó ra?

### Cầu nối sang Phase 6

Hive Metastore giờ là một service thật, nhiều engine cùng trỏ vào — nhưng nó vẫn chỉ
trả lời đúng một câu: *"bảng này ở đâu"*. Nó không biết **ai** được phép đọc
`gold.gold_daily_zone_revenue`, không ghi lại **bảng này từ đâu ra** (lineage), và
không có khái niệm "một tổ chức, nhiều catalog" — thứ Databricks thật cần khi một công
ty có hàng trăm team.

Đó là ba thứ Unity Catalog thêm vào, và Phase 6 sẽ tháo Hive Metastore ra để thay bằng
nó — đúng lúc bảng so sánh ở Phần 1 nói "Hive Metastore là đồ cũ" bắt đầu có ý nghĩa
thật, không còn là một dòng lý thuyết.
