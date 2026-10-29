from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'currency_pipeline',
    default_args=default_args,
    description='ETL pipeline: API -> MinIO -> Spark -> ClickHouse',
    schedule_interval='@hourly', # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False, # Don't run for past dates
    tags=['production', 'rates'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_pipeline'
    )

    # Task 1: Run ingestion script
    # We run it inside the Airflow container where pandas and boto3 are installed
    ingest_task = BashOperator(
        task_id='ingest_rates_data',
        bash_command='python /opt/airflow/ingestion/ingest.py',
    )

    # Task 2: Run Spark job
    # We use the docker socket to execute spark-submit on the spark-master container
    spark_transform_task = BashOperator(
        task_id='spark_transform_to_clickhouse',
        bash_command=(
            'docker exec dataengapp-spark-master-1 '
            '/opt/spark/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--conf spark.jars.ivy=/tmp/.ivy2 '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.4.6 '
            '/opt/spark/jobs/transform_rates.py'
        ),
    )

    end_task = EmptyOperator(
        task_id='end_pipeline'
    )

    # Define the execution order
    start_task >> ingest_task >> spark_transform_task >> end_task