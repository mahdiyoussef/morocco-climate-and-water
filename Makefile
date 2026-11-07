.DEFAULT_GOAL := help
PYTHON ?= python
COMPOSE ?= docker compose
PLATFORM = $(COMPOSE) --env-file .env.morocco -f docker-compose.yml
.PHONY: help init-env climate-download climate-verify era5-download process morocco-process dbt-build publish streamlit up-all morocco-up docker-pipeline docker-verify down morocco-down logs test morocco-test
help:
	@echo "Climate observatory: dbt, PostgreSQL, Power BI and Streamlit"
	@echo "make process      Verify layers and build/test dbt Gold"
	@echo "make publish      Publish tested local Gold to PostgreSQL"
	@echo "make streamlit    Serve the PostgreSQL dashboard"
	@echo "make up-all       Start the distributed platform"
	@echo "make docker-pipeline Run the complete DAG inside Docker"
	@echo "make docker-verify Verify MinIO, Spark, PostgreSQL and Streamlit"
	@echo "make test         Run application and platform checks"
init-env:
	$(PYTHON) observatory/setup_platform.py
climate-download:
	$(PYTHON) observatory/download_climate.py --start-year 2015 --end-year 2024
climate-verify:
	$(PYTHON) observatory/verify_climate.py
era5-download:
	$(PYTHON) observatory/download_era5.py --start-year 2015 --end-year 2024
process morocco-process:
	$(PYTHON) observatory/warehouse.py local-all
dbt-build:
	$(PYTHON) observatory/warehouse.py dbt
publish:
	$(PYTHON) observatory/serving.py publish-local
streamlit:
	$(PYTHON) -m streamlit run streamlit_app.py
up-all morocco-up: init-env
	$(PLATFORM) build spark-master
	$(PLATFORM) build
	$(PLATFORM) up -d --no-build
down morocco-down:
	$(PLATFORM) down
docker-pipeline:
	$(PLATFORM) exec airflow-scheduler airflow dags test morocco_observatory
docker-verify:
	$(PLATFORM) exec airflow-scheduler python -m observatory.verify_docker
logs:
	$(PLATFORM) logs -f
test morocco-test:
	$(PYTHON) -m pytest tests

.PHONY: screenshots
screenshots:
	$(PYTHON) scripts/capture_dashboard.py
