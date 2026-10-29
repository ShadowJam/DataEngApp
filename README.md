\# Airflow + Spark + ClickHouse Pet Project



An educational ETL pipeline: Ingestion from a public API -> Bronze (MinIO) -> Spark -> ClickHouse.



\## Stack

\- Apache Airflow 2.9.3

\- Apache Spark 3.5.0

\- ClickHouse 24.3

\- MinIO (S3-compatible object storage)

\- PostgreSQL (Airflow metadata)



\## Getting Started



1\. Copy `.env.example` to `.env` and fill in the values:

&#x20;  ```bash

&#x20;  cp .env.example .env

&#x20;  ```



2\. Bring up the infrastructure:

&#x20;  ```bash

&#x20;  docker compose up -d

&#x20;  ```



3\. Check the services:

&#x20;  - Airflow UI: http://localhost:8080 (airflow / airflow)

&#x20;  - MinIO Console: http://localhost:9001 (minioadmin / minioadmin)

&#x20;  - Spark Master UI: http://localhost:8081



\## Project Structure

\- `dags/` - Airflow DAGs

\- `spark\_jobs/` - PySpark transformation scripts

\- `ingestion/` - Python scripts for API extraction and loading to Bronze



\## Status

🚧 Work in progress. Infrastructure and Ingestion.

