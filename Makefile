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

shell: ## Vào shell của container jupyter
	docker compose exec jupyter bash

spark-shell: ## Mở pyspark shell nối vào cluster (gỡ lỗi nhanh)
	docker compose exec spark-connect /opt/spark/bin/pyspark --remote sc://localhost:15002

cluster: ## Kiểm tra cluster Spark: worker nào đang ALIVE
	@docker compose exec spark-master curl -s http://localhost:8080/json/ | \
	  python3 -c "import sys,json;d=json.load(sys.stdin);print(f\"Master {d['status']} — {d['aliveworkers']} worker ALIVE, {d['cores']} core, {d['memory']}MB\");[print('  •',w['id'],w['state'],w['cores'],'core') for w in d['workers']]"

.PHONY: help up down clean logs ps data shell spark-shell cluster
