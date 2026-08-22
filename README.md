# Databricks Clone — Học Data Engineering bằng Open Source

Dựng lại kiến trúc Databricks bằng các công nghệ mã nguồn mở, từng tầng một, để hiểu
data platform hiện đại hoạt động thế nào.

📍 **Đang ở: Phase 0 — Lát cắt mỏng** · Lộ trình đầy đủ: [docs/roadmap.md](docs/roadmap.md)

## Chạy thử

```bash
make up        # dựng stack (lần đầu build image, mất vài phút)
make data      # tải dữ liệu NYC Taxi (~50MB/tháng)
```

| Dịch vụ | Địa chỉ | Đăng nhập |
|---|---|---|
| JupyterLab | http://localhost:8888/lab?token=lakehouse | token: `lakehouse` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |

Mở `00_thin_slice.ipynb` trong JupyterLab và chạy từ trên xuống.

```bash
make          # xem tất cả lệnh
make down     # dừng, giữ dữ liệu
make clean    # dừng và xoá sạch dữ liệu MinIO
```

## Kiến trúc hiện tại

```
  Tầng 7 · Consumption    →  JupyterLab   ✅ đã dựng
  Tầng 6 · Orchestration  →  dbt+Airflow     Phase 4,7
  Tầng 5 · SQL warehouse  →  Trino           Phase 5
  Tầng 4 · Catalog        →  Unity Catalog   Phase 6
  Tầng 3 · Compute        →  Spark           Phase 3   (tạm dùng DuckDB)
  Tầng 2 · Table format   →  Delta Lake      Phase 2
  Tầng 1 · Object storage →  MinIO        ✅ đã dựng
```

## Cấu trúc thư mục

```
docker-compose.yml      stack — lớn dần qua mỗi phase
Makefile                lệnh tắt
.env                    cấu hình cổng và thông tin đăng nhập
docker/jupyter/         image notebook tuỳ biến
notebooks/              bài lab từng phase
scripts/                tiện ích (tải dữ liệu...)
data/                   dữ liệu thô (không commit)
docs/roadmap.md         lộ trình 12 phase
```

## Ghi chú môi trường

Đã kiểm tra trên macOS Apple Silicon (arm64). Mọi image đều có bản arm64 native —
không chạy emulate x86.
