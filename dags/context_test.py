from datetime import datetime
from datetime import timedelta
import pendulum
import logging

from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone('Asia/Irkutsk')

args = {
    'owner': 'Korsakova',
    'depends_on_past': True,
    'start_date': datetime(2022, 11, 21, tzinfo=local_tz),
    'retries': 3,
    'max_active_runs': 1,
    'provide_context': True,
    'catchup': True,
    'retry_delay': timedelta(hours=2),
}


def print_templates(**context):
    for i in context:
        print(f'key: {i}, value: {context[i]}, type: {type(context[i])}')


with DAG(
        dag_id='context_test',
        schedule_interval='10 10 * * *',
        default_args=args,
        max_active_runs=1,
) as dag:
    print_templates = PythonOperator(
        task_id='print_templates',
        python_callable=print_templates,
    )

    print_templates
