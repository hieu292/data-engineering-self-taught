# Databricks Clone — Học Data Engineering bằng Open Source

Dựng lại kiến trúc Databricks bằng các công nghệ mã nguồn mở, từng tầng một, để hiểu
data platform hiện đại hoạt động thế nào.

📍 **Đang ở: Phase 3 — Apache Spark** · Lộ trình đầy đủ: [docs/roadmap.md](docs/roadmap.md)

## Chạy thử

```bash
make up        # dựng stack (lần đầu build image Spark ~10 phút)
make data      # tải dữ liệu NYC Taxi 2024 (12 tháng, ~700MB)
```

| Dịch vụ | Địa chỉ | Đăng nhập |
|---|---|---|
| JupyterLab | http://localhost:8888/lab?token=lakehouse | token: `lakehouse` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark Master UI | http://localhost:8080 | — |
| Spark Application UI | http://localhost:4040 | — |

```bash
make              # xem tất cả lệnh
make cluster      # kiểm tra worker Spark nào đang ALIVE
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

Cách học chung cho cả bốn: **chạy cell → nhìn số của chính mình → rồi mới đọc giải thích.**
Mỗi phase đều có ít nhất một bước **sai có chủ đích** — làm hỏng trước, đo, rồi tự chữa.

## Kiến trúc hiện tại

```
  Tầng 7 · Consumption    →  JupyterLab      ✅ Phase 0
  Tầng 6 · Orchestration  →  dbt + Airflow      Phase 4, 7
  Tầng 5 · SQL warehouse  →  Trino              Phase 5
  Tầng 4 · Catalog        →  Unity Catalog      Phase 6
  Tầng 3 · Compute        →  Apache Spark    ✅ Phase 3
  Tầng 2 · Table format   →  Delta Lake      ✅ Phase 2
  Tầng 1 · Object storage →  MinIO           ✅ Phase 0
```

Sáu container đang chạy:

```
jupyter ──sc://15002──► spark-connect (DRIVER) ──7077──► spark-master
  pyspark-client              │                              │
  (không có JVM)          UI :4040                    ┌──────┴──────┐
                                                  worker-1      worker-2
                                                  2 core/3GB    2 core/3GB
                                                      │             │
                                                      └── s3a:// ───┴──► minio
```

Notebook **không chứa Spark**. Driver sống trong container `spark-connect`, nhận lệnh
qua gRPC. Đó là mô hình Databricks Connect — và là lý do image Jupyter cài
`pyspark-client` chứ không phải `pyspark`.

## Cấu trúc thư mục

```
docker-compose.yml      stack — lớn dần qua mỗi phase
Makefile                lệnh tắt
.env                    cấu hình cổng và thông tin đăng nhập (copy từ .env.example)
docker/jupyter/         image notebook — client mỏng
docker/spark/           image Spark — dùng chung cho master, worker, connect server
notebooks/              bài lab từng phase
scripts/                tiện ích (tải dữ liệu...)
data/                   dữ liệu thô (không commit)
docs/roadmap.md         lộ trình 12 phase + ghi chú chi tiết từng phase
```

## Ghi chú môi trường

Đã kiểm tra trên **macOS Apple Silicon (arm64)**. Mọi image đều có bản arm64 native —
không chạy emulate x86.

**RAM là ràng buộc thật, không phải RAM của máy bạn mà là của Docker.** Docker Desktop
mặc định chỉ cấp ~8GB dù máy có 32GB. Cấu hình Spark trong repo (2 executor × 2GB +
driver 1GB) được chọn để vừa trần đó. Vượt trần thì kernel giết executor (`exit 137`)
còn Spark báo nhầm thành lỗi shuffle (`MetadataFetchFailedException`) — rất khó lần ra.

Muốn chạy rộng tay hơn: Docker Desktop → Settings → Resources → Memory, rồi nâng
`SPARK_WORKER_MEMORY` trong `.env` và `spark.executor.memory` trong
`docker/spark/spark-defaults.conf`.

Version của mọi thành phần đều đã kiểm chứng trên registry thật, không phỏng đoán.
Bốn cái bẫy version đã gặp và cách gỡ được ghi lại ở
[Phần 6 của roadmap](docs/roadmap.md).
