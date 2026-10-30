from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 1. Добавлено двоеточие в конце
with DAG(
    'build_data_marts',
    default_args=default_args,
    description='Builds dimension and mart tables in ClickHouse',
    schedule_interval='@daily', # Usually marts are built once a day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['marts', 'analytics'],
) as dag:

    start_task = EmptyOperator(task_id='start')

    # Task 1 Create Dimension Table
    create_dim = SQLExecuteQueryOperator(
        task_id='create_dim_currencies',
        conn_id='clickhouse_default',
        sql='sql/marts/dim_currencies.sql',  # 2. Восстановлены слэши в пути
        split_statements=True,
    )

    # Task 2 Create Data Mart
    create_mart = SQLExecuteQueryOperator(
        task_id='create_mart_daily_rates',
        conn_id='clickhouse_default',
        sql='sql/marts/mart_daily_rates.sql', # 2. Восстановлены слэши в пути
        split_statements=True,
    )

    # 3. Восстановлена последовательность выполнения
    start_task >> create_dim >> create_mart