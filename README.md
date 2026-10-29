# Airflow + Spark + ClickHouse Pet Project

An end-to-end, containerized ETL pipeline orchestrated by Apache Airflow. This project demonstrates data ingestion, distributed processing, and analytical storage using modern Data Engineering tools.

## Stack
- **Orchestration**: Apache Airflow 2.9.3
- **Processing**: Apache Spark 3.5.0
- **Storage (Data Lake)**: MinIO (S3-compatible)
- **Storage (Analytics)**: ClickHouse 24.3
- **Metadata DB**: PostgreSQL 13
- **Infrastructure**: Docker & Docker Compose

## Architecture & Data Flow
1. **Ingestion**: A Python script (`ingestion/ingest.py`) fetches currency exchange rates from the [Frankfurter API](https://www.frankfurter.app/), converts them to Parquet, and saves them to the `bronze` bucket in MinIO.
2. **Transformation**: A PySpark job (`spark_jobs/transform_rates.py`) reads the raw Parquet data from MinIO, calculates the average rate per currency, and writes the result to ClickHouse.
3. **Orchestration**: An Airflow DAG (`dags/currency_pipeline.py`) schedules and executes the Ingestion and Transformation steps hourly, handling retries and dependencies.

## Project Structure
- `dags/` - Airflow DAG definitions
- `ingestion/` - Python scripts for API extraction and loading to MinIO
- `spark_jobs/` - PySpark transformation scripts
- `clickhouse/` - ClickHouse initialization SQL scripts
- `Dockerfile.airflow` - Custom Airflow image with required dependencies and Docker CLI

## Getting Started

### 1. Prerequisites
- Docker Desktop (with WSL 2 backend on Windows)
- Git (optional)

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
Bring up the entire stack (Airflow, Spark, MinIO, ClickHouse, Postgres). The `airflow-init` service will automatically initialize the metadata database and create the default admin user (`airflow`/`airflow`).
```bash
docker compose up -d
```

### 4. Verify Services
- **Airflow UI**: http://localhost:8080 (user: `airflow`, password: `airflow`)
- **MinIO Console**: http://localhost:9001 (user: `minioadmin`, password: `minioadmin`)
- **Spark Master UI**: http://localhost:8081

## Running the Pipeline

1. Open the Airflow UI.
2. Locate the `currency_pipeline` DAG.
3. Toggle it to **On** and click the **Play** button (Trigger DAG) to run it manually.
4. You can monitor the progress and check logs directly in the Airflow UI.

The DAG is scheduled to run `@hourly`, but `catchup` is set to `False` to prevent backfilling past dates.

## Verifying the Data

To verify the data landed in ClickHouse, you can use the CLI. Run the following command from the project root to connect (it automatically reads the password from the container's environment variables):

```bash
docker exec -it dataengapp-clickhouse-1 sh -c 'clickhouse-client --password $CLICKHOUSE_PASSWORD'
```

Once inside the ClickHouse client, run this SQL query:
```sql
SELECT * FROM rates_db.fact_rates LIMIT 10;
```

## Key Features & Best Practices
- **Configuration Management**: All credentials and environment-specific settings are managed via `.env` files and passed safely to containers.
- **Error Handling**: Ingestion uses `tenacity` for exponential backoff. Airflow DAG uses `retries` and `retry_delay` for fault tolerance.
- **Idempotency**: Airflow uses `catchup=False`, and ClickHouse uses the `MergeTree` engine.
- **Docker Integration**: Airflow uses the mounted `docker.sock` to trigger Spark jobs seamlessly within the same Docker network.