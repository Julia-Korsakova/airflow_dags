import logging

from datetime import timedelta
from datetime import datetime

import requests
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

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


def get_random_joke() -> pd.DataFrame:
    """
    :return: DataFrame 'df' с рандомной шуткой
    """

    url = 'https://official-joke-api.appspot.com/jokes/random'
    try:
        logging.info('Попытка получения данных через API')
        response = requests.get(url).json()
        df = pd.DataFrame.from_dict(response, orient='index').T
        logging.info(f'ОК: Получено данных {len(df)}')
        return df
    except Exception as exception:
        logging.warning(f'{DAG_ID} Не удалось получить данные через API')
        raise exception


def write_temp_data() -> None:
    """
    Получает данные и записывает их во временную таблицу tmp_random_joke
    """

    df = get_random_joke()

    try:
        pg_hook = PostgresHook('pgsql_db')
        df.to_sql(
            name='tmp_random_joke',
            con=pg_hook.get_uri(),
            schema=TARGET_SCHEME,
            if_exists='append',
            index=False,
        )
    except Exception as exception:
        logging.warning(f'{DAG_ID} Не удалось записать данные во временную таблицу')
        raise exception


with DAG(
        dag_id=DAG_ID,
        schedule_interval='0 */2 * * *',
        default_args=args,
        catchup=True,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    drop_tmp_random_joke = PostgresOperator(
        task_id='drop_tmp_random_joke',
        postgres_conn_id='pgsql_db',
        sql='''
            DROP TABLE IF EXISTS public.tmp_random_joke
            '''
    )
    drop_tmp_random_joke.doc_md = 'Удаление временной таблицы "tmp_random_joke", если она существует'

    write_temp_data = PythonOperator(
        task_id='insert_temp_data',
        python_callable=write_temp_data,
    )

    insert_new_values = PostgresOperator(
        task_id='insert_new_values',
        postgres_conn_id='pgsql_db',
        sql='''
            INSERT INTO public.random_joke
            (
                id,
                type,
                setup,
                punchline
            )
            SELECT 
                id,
                type,
                setup,
                punchline
            FROM 
                public.tmp_random_joke
            ON CONFLICT (id) DO NOTHING
            '''
    )
    insert_new_values.doc_md = 'Вставка данных в таблицу PGSql'

    drop_tmp_random_joke_end = PostgresOperator(
        task_id='drop_tmp_random_joke_end',
        postgres_conn_id='pgsql_db',
        sql='''
            DROP TABLE IF EXISTS public.tmp_random_joke
            '''
    )
    drop_tmp_random_joke_end.doc_md = 'Удаление временной таблицы "tmp_random_joke", если она существует'

    drop_tmp_random_joke >> write_temp_data >> insert_new_values >> drop_tmp_random_joke_end
