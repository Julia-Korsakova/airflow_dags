import logging

from datetime import timedelta
from datetime import datetime

import requests
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


TARGET_TABLE = 'random_joke'
TARGET_SCHEME = 'public'
DAG_ID = 'random_joke'

local_tz = pendulum.timezone('Asia/Irkutsk')

args = {
    'owner': 'Korsakova',
    'start_date': datetime(2023, 2, 26, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(hours=2),
}


def get_DataFrame() -> pd.DataFrame:
    """
    :return: df с рандомной шуткой
    """

    url = 'https://official-joke-api.appspot.com/jokes/random'
    try:
        logging.info('Попытка получения данных из API')
        response = requests.get(url).json()
        df = pd.DataFrame.from_dict(response, orient='index').T
        logging.info('ОК: Получили данные')
    except Exception as exception:
        logging.warning(f'{DAG_ID} Не удалось получить данные через API')
        raise exception
    return df


def insert_new_data():
    '''
    :return:
    '''

    df = get_DataFrame()
    print(df)

    pg_hook = PostgresHook('pgsql_db')

    try:
        logging.info('Попытка вставки данных')
        df.to_sql(
            name=TARGET_TABLE,
            con=pg_hook.get_uri(),
            schema=TARGET_SCHEME,
            if_exists='append',
            index=False,
        )
        logging.info('OK: Данные успешно вставлены')
    except Exception as exception:
        logging.warning(f'{DAG_ID} Не удалось вставить данные')
        raise exception


with DAG(
        dag_id=DAG_ID,
        schedule_interval='0 */4 * * *',
        default_args=args,
        catchup=True,
) as dag:
    insert_new_data = PythonOperator(
        task_id='insert_new_data',
        python_callable=insert_new_data,
    )
    insert_new_data.doc_md = 'Вставка данных в таблицу PGSql'
