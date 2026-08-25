# Databricks Clone — Học Data Engineering bằng Open Source

Dựng lại kiến trúc Databricks bằng các công nghệ mã nguồn mở, từng tầng một, để hiểu
data platform hiện đại hoạt động thế nào.

📍 **Đang ở: Phase 6 — Unity Catalog** · Lộ trình đầy đủ: [docs/roadmap.md](docs/roadmap.md)

## Chạy thử

```bash
make up        # dựng stack (lần đầu build image Spark ~10 phút)
make data      # tải dữ liệu NYC Taxi 2024 (12 tháng, ~700MB)
make ingest    # raw → bronze (bảng Delta có tên, 41,1 triệu dòng)
make dbt       # bronze → silver → gold, kèm 16 bài test
```

| Dịch vụ | Địa chỉ | Đăng nhập |
|---|---|---|
| JupyterLab | http://localhost:8888/lab?token=lakehouse | token: `lakehouse` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark Master UI | http://localhost:8080 | — |
| Spark Application UI | http://localhost:4040 | — |
| dbt docs + lineage | http://localhost:8081 | sau khi chạy `make dbt-docs` |
| Trino | http://localhost:8082 | — (`make trino-shell` để vào CLI) |
| Superset | http://localhost:8088 | `admin` / xem `SUPERSET_ADMIN_PASSWORD` trong `.env` |
| Unity Catalog | http://localhost:8089 | PAT tự ký — `make uc-token` |

```bash
make              # xem tất cả lệnh
make cluster      # kiểm tra worker Spark nào đang ALIVE
make dbt-docs     # sinh tài liệu + sơ đồ lineage
make dbt-shell    # vào container dbt, chạy lệnh dbt tuỳ ý
make trino-shell  # vào Trino CLI, catalog delta sẵn
make uc-register  # đăng ký metadata bảng thật vào Unity Catalog (sau 'make dbt')
make uc-token     # tự ký PAT Unity Catalog mới (PRINCIPAL=... để đổi principal)
make down         # dừng, giữ dữ liệu
make clean        # dừng và xoá sạch dữ liệu MinIO
```

## Các bài lab

Chạy theo thứ tự — mỗi notebook dựa trên thứ notebook trước để lại.

| Notebook | Nội dung |
|---|---|
| `00_thin_slice.ipynb` | Lát cắt mỏng: MinIO + DuckDB + Jupyter nói chuyện được với nhau |
| `01_file_formats.ipynb` | CSV/JSON/Parquet, row group, partition pruning, small files problem |
| `02_delta_lake.ipynb` | `_delta_log` đọc bằng tay, ACID không cần khoá, time travel, `MERGE`, `VACUUM` |
| `03_spark.ipynb` | driver/executor, lazy evaluation, shuffle, data skew, broadcast join |
| `04_medallion_dbt.ipynb` | bronze/silver/gold, model + test dbt, lineage, idempotency |
| `05_trino_superset.ipynb` | SQL qua Trino không cần Spark, catalog.schema.table, dashboard Superset |
| `06_unity_catalog.ipynb` | Catalog ba tầng, phân quyền GRANT thật, và giới hạn thật của UC OSS với MinIO |

Cách học chung cho cả năm: **chạy cell → nhìn số của chính mình → rồi mới đọc giải thích.**
Mỗi phase đều có ít nhất một bước **sai có chủ đích** — làm hỏng trước, đo, rồi tự chữa.

## Kiến trúc hiện tại

```
  Tầng 7 · Consumption    →  JupyterLab      ✅ Phase 0   (+ Superset: Phase 5)
  Tầng 6 · Transform      →  dbt             ✅ Phase 4   (Airflow: Phase 7)
  Tầng 5 · SQL warehouse  →  Trino           ✅ Phase 5
  Tầng 4 · Catalog        →  Unity Catalog   ✅ Phase 6   (governance — dữ liệu thật vẫn qua Hive Metastore, xem dưới)
  Tầng 3 · Compute        →  Apache Spark    ✅ Phase 3
  Tầng 2 · Table format   →  Delta Lake      ✅ Phase 2
  Tầng 1 · Object storage →  MinIO           ✅ Phase 0
```

Mười hai container đang chạy:

```
jupyter ──sc://15002──┐
  pyspark-client      │
  (không có JVM)      ├──► spark-connect (DRIVER) ──7077──► spark-master
                      │          │      │                       │
dbt   ──sc://15002────┘      UI :4040   │                ┌──────┴──────┐
  dbt-core + adapter                    │             worker-1      worker-2
  (không có JVM)                        │             2 core/3GB    2 core/3GB
                                         │                   │             │
trino ──HTTP :8080──┐                   │                   └── s3a:// ───┴──► minio
  (engine SQL riêng, │                  │
   không qua Spark)  │                  ├──► hive-metastore ──9083──┐
                      ├──► hive-metastore   (Thrift — DỮ LIỆU thật)  ├──► postgres
                      │                                              │   (metastore_db +
superset ──sqlalchemy-trino──► trino                                 │    unitycatalog_db +
  (dashboard, đọc metadata riêng) ────────────────────────────────────┘    superset_meta)
                                         │
                      unity.* (browse)  ▼
                      spark-connect ──REST :8080──► unity-catalog
                                         (GOVERNANCE — metadata thôi, xem "Một catalog" dưới)
```

**Hai client, một driver.** Cả notebook lẫn dbt đều không chứa Spark — không JVM, không
tính toán. Chúng gửi mô tả phép tính qua gRPC tới container `spark-connect`. Đó là mô hình
Databricks Connect, và là lý do cả hai image đều cài `pyspark-client` chứ không phải `pyspark`.

**Một catalog cho dữ liệu, một catalog cho governance.** `spark-connect` và `trino` đọc-ghi
dữ liệu thật qua `hive-metastore` (Thrift, ghi vào Postgres) — không đổi từ Phase 5. Kế
hoạch Phase 6 là thay hẳn bằng Unity Catalog; thử thật thì vỡ: `unitycatalog-spark` luôn tự
xin Unity Catalog "vend" credential S3 tạm thời cho mọi thao tác chạm dữ liệu, và bản UC OSS
phát hành chính thức (v0.6.0) chưa hỗ trợ custom S3 endpoint (MinIO) trong luồng đó — MinIO
từ chối thẳng token giả với `InvalidTokenId` (tự tay kiểm bằng `aws-cli`, không phải suy
đoán). Quyết định thật: Unity Catalog đứng CẠNH làm tầng governance thật — catalog ba tầng,
GRANT/REVOKE có tác dụng thật — nạp bằng cách đăng ký metadata các bảng thật qua REST API
(`scripts/register_unity_catalog.py`), không qua Spark. Chi tiết đầy đủ:
[Phần 9 của roadmap](docs/roadmap.md) và `notebooks/06_unity_catalog.ipynb`.

## Kiến trúc dữ liệu — medallion

```
raw/            bronze              silver               gold
────────        ──────────────      ────────────────     ──────────────
Parquet    →    nguyên trạng   →    sạch, đúng kiểu  →   trả lời câu hỏi
của TLC         + 3 cột `_`         1 dòng = 1 chuyến    đã tổng hợp

41.169.720      41.169.720          35.613.229           80.523 + 1.300

make ingest     ────────── make dbt lo từ đây trở đi ──────────►
```

**13,5% dữ liệu gốc là rác** và bị chặn lại ở cửa silver — đo bằng chính notebook, không
phải con số nghe kể. dbt **không nạp dữ liệu**: nó là chữ T trong ELT.

## Cấu trúc thư mục

```
docker-compose.yml      stack — lớn dần qua mỗi phase
Makefile                lệnh tắt
.env                    cấu hình cổng và thông tin đăng nhập (copy từ .env.example)
docker/jupyter/         image notebook — client mỏng
docker/spark/           image Spark — dùng chung cho master, worker, connect server
docker/dbt/             image dbt — dbt-core + adapter, cũng là client mỏng
docker/hive-metastore/  image Hive Metastore độc lập (Thrift) — nơi Spark/dbt/Trino đọc-ghi bảng thật
docker/postgres/        script khởi tạo 3 DB (metastore + unitycatalog + superset trong 1 Postgres)
docker/trino/           cấu hình Trino — catalog delta, không cần build image riêng
docker/superset/        image Superset — thêm driver Postgres + Trino
docker/unity-catalog/   image Unity Catalog OSS — tầng governance (Phase 6), không phục vụ dữ liệu
dbt/                    dự án dbt: model, test, seed, macro
notebooks/              bài lab từng phase
scripts/                tiện ích (tải dữ liệu, nạp bronze, đăng ký/ký token Unity Catalog...)
data/                   dữ liệu thô (không commit)
docs/roadmap.md         lộ trình 12 phase + ghi chú chi tiết từng phase
```

## Ghi chú môi trường

Đã kiểm tra trên **macOS Apple Silicon (arm64)**. Mọi image đều có bản arm64 native —
không chạy emulate x86. Riêng `apache/hive:4.0.0` (Phase 5) mới xác nhận qua manifest
đa kiến trúc trên Docker Hub, chưa `docker image inspect` trực tiếp trên máy arm64 —
ba image còn lại của phase này (`postgres:17`, `trinodb/trino:476`,
`apache/superset:4.1.1`) đã nằm sẵn trong bảng đã pull thật ở
[Phần 1 của roadmap](docs/roadmap.md).

**RAM là ràng buộc thật, không phải RAM của máy bạn mà là của Docker.** Docker Desktop
mặc định chỉ cấp ~8GB dù máy có 32GB — đủ cho Phase 0-4. Từ Phase 5, cả stack (thêm
Postgres, Hive Metastore, Trino, Superset, và từ Phase 6 thêm Unity Catalog — nhẹ,
~350MB) đo thật khoảng **~9-10GB** lúc chạy đồng thời — **nâng trần Docker lên ít nhất
12-16GB** trước khi `make up`. Vượt trần thì kernel giết tiến trình (`exit 137`); với
Spark, triệu chứng là lỗi shuffle rất khó hiểu (`MetadataFetchFailedException`) chứ
không báo thẳng là hết RAM.

Muốn chạy rộng tay hơn: Docker Desktop → Settings → Resources → Memory, rồi nâng
`SPARK_WORKER_MEMORY` trong `.env` và `spark.executor.memory` trong
`docker/spark/spark-defaults.conf`. `docker/trino/etc/jvm.config` (`-Xmx2G`) và
`SERVICE_OPTS` của `hive-metastore` (mặc định `-Xmx1G` do ảnh gốc đặt) cũng đáng
chỉnh nếu Trino/Metastore chạy chậm.

Version của mọi thành phần đều đã kiểm chứng trên registry thật, không phỏng đoán.
Bốn cái bẫy version đã gặp và cách gỡ được ghi lại ở
[Phần 6 của roadmap](docs/roadmap.md).
