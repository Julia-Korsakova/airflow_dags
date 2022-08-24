import logging
from datetime import datetime, timedelta

import pymssql
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

local_tz = pendulum.timezone('Asia/Vladivostok')

args = {
    'owner': 'Korsakova',
    'depends_on_past': False,
    # 'email': [Variable.get("default_email_to")],
    'email_on_failure': True,
    'email_on_retry': True,
    'parallel_task_count': 5,
    'start_date': datetime(2021, 10, 30, tzinfo=local_tz),
    'retries': 3,
    'max_active_runs': 2,
    'provide_context': True,
    'retry_delay': timedelta(hours=2),
}

def check():
    logging.info('Hello world')

def connection():
    logging.info('String connection')

with DAG(
        dag_id='_check_connections',
        schedule_interval='@daily',
        default_args=args,
        # catchup=False,
) as dag:

    check = PythonOperator(
        task_id='check',
        python_callable=check,
    )
    check.doc_md = " 111 "

    connection = PythonOperator(
        task_id='connection',
        python_callable=connection,
    )
    connection.doc_md = " 222 "

    check >> connection
