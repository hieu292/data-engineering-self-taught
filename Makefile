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

.PHONY: help up down clean logs ps data shell
