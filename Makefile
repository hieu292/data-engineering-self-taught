.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Hiện danh sách lệnh
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

up: ## Khởi động toàn bộ stack
	docker compose up -d --build
	@echo ""
	@echo "  MinIO console : http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  JupyterLab    : http://localhost:8888/lab?token=lakehouse"
	@echo "  Spark master  : http://localhost:8080   (xem worker nào còn sống)"
	@echo "  Spark app UI  : http://localhost:4040   (đọc job/stage/shuffle)"
	@echo "  dbt docs      : http://localhost:8081   (sau khi chạy 'make dbt-docs')"
	@echo "  Trino UI      : http://localhost:8082   (SHOW SCHEMAS FROM delta)"
	@echo "  Superset      : http://localhost:8088   (admin / xem SUPERSET_ADMIN_PASSWORD trong .env)"

down: ## Dừng stack (giữ dữ liệu)
	docker compose down

clean: ## Dừng stack và XOÁ SẠCH dữ liệu trong MinIO
	docker compose down -v

logs: ## Xem log realtime
	docker compose logs -f

ps: ## Trạng thái các service
	docker compose ps

data: ## Tải dữ liệu NYC Taxi về thư mục data/
	./scripts/download_data.sh

ingest: ## Nạp raw → bronze (MONTH=2024-03 để nạp một tháng)
	docker compose exec dbt python /scripts/ingest_bronze.py $(MONTH)

dbt: ## Chạy toàn bộ pipeline dbt: seed + model + test
	@# --exclude sandbox: thư mục sandbox chứa model CỐ TÌNH sai của bước 6
	@# (incremental + append). Để nó trong lượt chạy thường thì mỗi lần `make dbt`
	@# lại nhân đôi dữ liệu trong đó — đúng cái bẫy nó dùng để minh hoạ.
	docker compose exec -w /dbt dbt dbt build --exclude sandbox

dbt-trap: ## Chạy model CỐ TÌNH sai của bước 6 (chạy nhiều lần để thấy dữ liệu nhân lên)
	docker compose exec -w /dbt dbt dbt run --select sandbox

dbt-test: ## Chỉ chạy test, không dựng lại bảng
	docker compose exec -w /dbt dbt dbt test

dbt-docs: ## Sinh tài liệu + sơ đồ lineage rồi mở ở http://localhost:8081
	docker compose exec -w /dbt dbt dbt docs generate
	@docker compose exec -T dbt pkill -f http.server 2>/dev/null || true
	@# `dbt docs serve` chỉ phục vụ tĩnh thư mục target/ — nên dùng thẳng http.server.
	@# Hai lý do bỏ `dbt docs serve`: nó mặc định chỉ nghe 127.0.0.1 (cổng ánh xạ ra
	@# host không nối tới đâu), và nó KHÔNG đặt SO_REUSEADDR nên chạy lần thứ hai là
	@# "Address already in use" vì cổng còn kẹt TIME_WAIT. http.server không dính cả hai.
	docker compose exec -w /dbt/target -d dbt python -m http.server 8081 --bind 0.0.0.0
	@echo "  dbt docs → http://localhost:8081  (bấm hình tròn xanh góc dưới phải để xem lineage)"

dbt-shell: ## Vào shell container dbt (chạy lệnh dbt tuỳ ý)
	docker compose exec -w /dbt dbt bash

shell: ## Vào shell của container jupyter
	docker compose exec jupyter bash

spark-shell: ## Mở pyspark shell nối vào cluster (gỡ lỗi nhanh)
	docker compose exec spark-connect /opt/spark/bin/pyspark --remote sc://localhost:15002

trino-shell: ## Mở Trino CLI, catalog delta sẵn (gỡ lỗi nhanh)
	docker compose exec trino trino --catalog delta

cluster: ## Kiểm tra cluster Spark: worker nào đang ALIVE
	@docker compose exec spark-master curl -s http://localhost:8080/json/ | \
	  python3 -c "import sys,json;d=json.load(sys.stdin);print(f\"Master {d['status']} — {d['aliveworkers']} worker ALIVE, {d['cores']} core, {d['memory']}MB\");[print('  •',w['id'],w['state'],w['cores'],'core') for w in d['workers']]"

.PHONY: help up down clean logs ps data ingest dbt dbt-trap dbt-test dbt-docs dbt-shell shell spark-shell trino-shell cluster
