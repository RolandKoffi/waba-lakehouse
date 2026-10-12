SUBMIT = docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
	--conf spark.driver.host=spark-master --conf spark.cores.max=2 --conf spark.executor.memory=1g
COUNTRIES ?= ALL

.PHONY: env up up-all down referentials transactions ingest reingest-archive bronze silver gold report trino test lint

env:
	test -f .env || cp .env.example .env

up: env
	docker compose up -d --build

up-all: env
	docker compose --profile orchestration --profile streaming up -d --build

down:
	docker compose --profile orchestration --profile streaming down

referentials:
	docker compose exec generator python data_gen.py --referentials

transactions:
	docker compose exec generator python data_gen.py --dataset bank_transactions --rows 10000
	docker compose exec generator python data_gen.py --dataset insurance_operations --rows 5000
	docker compose exec generator python data_gen.py --dataset mobile_money_payments --rows 20000
	docker compose exec generator python data_gen.py --dataset loan_repayments --rows 5000

ingest:
	$(SUBMIT) /opt/jobs/ingest_raw.py --countries $(COUNTRIES)

reingest-archive:
	$(SUBMIT) /opt/jobs/ingest_raw.py --bucket archive --no-archive --countries $(COUNTRIES)

bronze:
	$(SUBMIT) /opt/jobs/raw_to_bronze.py --countries $(COUNTRIES)

silver:
	$(SUBMIT) /opt/jobs/bronze_to_silver.py --countries $(COUNTRIES)

gold:
	$(SUBMIT) /opt/jobs/silver_to_gold.py --countries $(COUNTRIES)

report:
	$(SUBMIT) /opt/jobs/regulatory_report.py --countries $(COUNTRIES)

trino:
	docker compose exec trino trino --catalog iceberg

test:
	python tests/test_local_pipeline.py

lint:
	flake8 --max-line-length 120 generator spark/jobs airflow/dags nifi tests
