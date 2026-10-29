# Airflow + Spark + ClickHouse Pet Project

An educational ETL pipeline: Ingestion from a public API -> Bronze (MinIO) -> Spark -> ClickHouse.

## Stack
- Apache Airflow 2.9.3
- Apache Spark 3.5.0
- ClickHouse 24.3
- MinIO (S3-compatible object storage)
- PostgreSQL (Airflow metadata)

## Architecture & Data Flow
1. **Ingestion**: A Python script fetches currency exchange rates from the Frankfurter API and saves the raw data as Parquet files into the `bronze` bucket in MinIO.
2. **Transformation (Spark)**: A PySpark job reads the Parquet files from MinIO, aggregates the rates, and writes the result into a ClickHouse table.
3. **Serving (ClickHouse)**: ClickHouse stores the final aggregated data in a `MergeTree` table, ready for analytics.

## Getting Started

1. Copy `.env.example` to `.env` and fill in the values:
   ```bash
   cp .env.example .env
   ```

2. Bring up the infrastructure:
   ```bash
   docker compose up -d
   ```

3. Check the services:
   - Airflow UI: http://localhost:8080 (airflow / airflow)
   - MinIO Console: http://localhost:9001 (minioadmin / minioadmin)
   - Spark Master UI: http://localhost:8081

## Project Structure
- `dags/` - Airflow DAGs
- `spark_jobs/` - PySpark transformation scripts
- `ingestion/` - Python scripts for API extraction and loading to Bronze
- `clickhouse/` - ClickHouse initialization SQL scripts

## Ingestion Layer

The ingestion script (`ingestion/ingest.py`) fetches the latest currency exchange rates from the public [Frankfurter API](https://www.frankfurter.app/).

### Key Features
- **Retries with Exponential Backoff**: Uses `tenacity` to handle temporary network failures gracefully.
- **In-Memory Processing**: Converts JSON data to Pandas DataFrame and writes it as Parquet directly to MinIO using an in-memory buffer (`io.BytesIO`), avoiding local disk I/O.
- **Auto-Bucket Creation**: Automatically checks if the `bronze` bucket exists and creates it if necessary.
- **Partitioning**: Saves data in a Hive-style partitioned folder structure (`date=YYYY-MM-DD`) to optimize downstream Spark reads.

### How to Run Locally
Ensure your virtual environment is activated and dependencies are installed, then run:
```bash
python ingestion/ingest.py
```

## Spark Transformation Layer

The PySpark job (`spark_jobs/transform_rates.py`) reads the raw Parquet data from MinIO, calculates the average exchange rate per currency for each date, and writes the result to ClickHouse.

### How to Run
To execute the Spark job inside the Docker cluster, run the following command from the project root:

```bash
docker exec -it dataengapp-spark-master-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.4.6 \
  /opt/spark/jobs/transform_rates.py
```

## ClickHouse Layer

ClickHouse is initialized automatically using the `clickhouse/init.sql` script (mounted via Docker volume). This script creates the `rates_db` database and the `fact_rates` table.

### Connecting
To connect to ClickHouse via CLI:
```bash
docker exec -it dataengapp-clickhouse-1 clickhouse-client --password clickhouse
```

### Verifying Data
```sql
SELECT * FROM rates_db.fact_rates LIMIT 10;
```

## Status
Work in progress. Infrastructure, Ingestion, and Spark transformation are complete. Next step: Airflow orchestration.