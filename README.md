# Airflow + Spark + ClickHouse Pet Project



ETL pipeline: Ingestion from a public API -> Bronze (MinIO) -> Spark -> ClickHouse.



## Stack

- Apache Airflow 2.9.3

- Apache Spark 3.5.0

- ClickHouse 24.3

- MinIO (S3-compatible object storage)

- PostgreSQL (Airflow metadata)



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

- `spark\_jobs/` - PySpark transformation scripts

- `ingestion/` - Python scripts for API extraction and loading to Bronze



## Status

🚧 Work in progress. Infrastructure and Ingestion.

