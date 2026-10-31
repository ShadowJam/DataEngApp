# Airflow + Spark + ClickHouse Pet Project

An end-to-end, containerized Data Engineering pipeline featuring both **Batch** and **Streaming** processing. This project demonstrates data ingestion, distributed processing, analytical storage, and SQL data modeling using modern tools.

## Stack
- **Orchestration**: Apache Airflow 2.11.0
- **Processing**: Apache Spark 3.5.0 (Batch & Structured Streaming)
- **Streaming Broker**: Redpanda (Kafka API)
- **Storage (Data Lake)**: MinIO (S3-compatible)
- **Storage (Analytics)**: ClickHouse 24.3
- **Metadata DB**: PostgreSQL 13
- **Infrastructure**: Docker & Docker Compose

## Architecture & Data Flow

This project implements two distinct pipelines:

### 1. Batch Pipeline (Fully Automated)
- **Ingestion**: A Python script (`ingestion/ingest.py`) fetches currency exchange rates from the [Frankfurter API](https://www.frankfurter.app/), converts them to Parquet, and saves them to the `bronze` bucket in MinIO.
- **Transformation**: A PySpark job (`spark_jobs/transform_rates.py`) reads the raw Parquet data from MinIO, calculates the average rate per currency, and writes the result to ClickHouse (`fact_rates` table).
- **Data Marts**: An Airflow DAG (`build_marts.py`) runs SQL scripts to build a dimension table (`dim_currencies`) and an analytical mart (`mart_daily_rates`) with window functions for day-over-day percentage change.
- **Orchestration**: Airflow schedules and executes the Ingestion, Spark Transformation, and Data Mart creation hourly.

### 2. Streaming Pipeline (Semi-Automated)
- **Producer**: A Python script (`ingestion/kafka_producer.py`) simulates live currency events and sends them to a Redpanda topic.
- **Consumer**: A Spark Structured Streaming job (`spark_jobs/stream_rates.py`) reads from Redpanda, aggregates rates in 1-minute tumbling windows, and streams the results into ClickHouse (`streaming_rates` table) via `foreachBatch`.

## Project Structure
- `dags/` - Airflow DAGs (Batch orchestration)
- `ingestion/` - Python scripts (API ingestion, Kafka producer)
- `spark_jobs/` - PySpark scripts (Batch transformation, Streaming processing)
- `sql/marts/` - SQL scripts for building Data Marts (Star Schema)
- `clickhouse/` - ClickHouse initialization SQL (DDL for all tables)
- `Dockerfile.airflow` - Custom Airflow image with required dependencies

## Getting Started

### 1. Prerequisites
- Docker Desktop (with WSL 2 backend on Windows)
- Git

### 2. Environment Setup
Copy the example environment file and fill in the values:
```bash
cp .env.example .env
```

Ensure your `.env` file contains the following key variables:
```env
# Airflow
AIRFLOW_FERNET_KEY=your_generated_fernet_key
AIRFLOW_UID=50000

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000

# ClickHouse
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse
```

### 3. Start the Infrastructure
Build the custom Airflow image (this installs all Python dependencies) and start the stack. The `airflow-init` service will automatically initialize the metadata database.
```bash
docker compose build
docker compose up -d
```

### 4. Verify Services
- **Airflow UI**: http://localhost:8080 (user: `airflow`, password: `airflow`)
- **MinIO Console**: http://localhost:9001 (user: `minioadmin`, password: `minioadmin`)
- **Spark Master UI**: http://localhost:8081

## Running the Pipelines

### Batch Pipeline
1. Open the Airflow UI.
2. Turn **On** the `currency_pipeline` and `build_data_marts` DAGs.
3. Trigger them manually or wait for the hourly schedule. You can monitor the progress and check logs directly in the Airflow UI.

### Streaming Pipeline
Because streaming requires continuous processes, it is run semi-automatically via local scripts:
1. Ensure the virtual environment is activated and dependencies are installed (`pip install kafka-python`).
2. Run the producer locally:
   ```bash
   python ingestion/kafka_producer.py
   ```
3. In a new terminal, submit the Spark Streaming job:
   ```bash
   docker exec -it dataengapp-spark-master-1 /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.3.1 \
     /opt/spark/jobs/stream_rates.py
   ```

## Verifying the Data

To verify the data landed in ClickHouse, connect via CLI. The command automatically reads the password from the container's environment variables:
```bash
docker exec -it dataengapp-clickhouse-1 sh -c 'clickhouse-client --password $CLICKHOUSE_PASSWORD'
```

Once inside the ClickHouse client, you can run queries for different tables:
```sql
-- Batch fact table
SELECT * FROM rates_db.fact_rates LIMIT 10;

-- Data Mart (joined with dimension)
SELECT * FROM rates_db.mart_daily_rates ORDER BY date DESC LIMIT 10;

-- Streaming results
SELECT * FROM rates_db.streaming_rates ORDER BY window_start DESC LIMIT 10;
```

## Key Features & Best Practices
- **Configuration Management**: Credentials are managed via `.env` files and passed safely to containers (no hardcoded passwords in Spark jobs).
- **Idempotency**: `ReplacingMergeTree` for dimension tables, safe window functions in marts, and Airflow `catchup=False`.
- **Data Quality (DQ)**: Deduplication in Data Marts to ensure BI tools get clean data.
- **Docker Integration**: Airflow uses the mounted `docker.sock` to trigger Spark jobs seamlessly within the same network.