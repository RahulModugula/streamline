.PHONY: up down producer spark-job create-topics lint test

KAFKA_BOOTSTRAP := localhost:9092
TOPIC           := github-events
CHECKPOINT_DIR  := /tmp/streamline-checkpoints
OUTPUT_DIR      := ./data/lake

up:
	docker-compose up -d
	@echo "Waiting for Kafka..."
	@sleep 10
	$(MAKE) create-topics

down:
	docker-compose down -v

create-topics:
	docker-compose exec kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic $(TOPIC) \
		--partitions 4 \
		--replication-factor 1 \
		--config retention.ms=86400000

producer:
	cd producer && pip install -q -r requirements.txt && \
	python github_producer.py

spark-job:
	cd spark && pip install -q -r requirements.txt && \
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		streaming_job.py

lint:
	cd producer && python -m py_compile github_producer.py
	cd spark && python -m py_compile streaming_job.py schema.py

test:
	cd tests && pip install -q -r requirements.txt && pytest -v
