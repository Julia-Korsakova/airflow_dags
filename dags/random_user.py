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

TARGET_TABLE = 'random_user'
TARGET_SCHEME = 'public'
DAG_ID = 'random_user'
CONN_ID = 'pgsql_db'

local_tz = pendulum.timezone('Asia/Irkutsk')

args = {
    'owner': 'Korsakova',
    'start_date': datetime(2023, 3, 2, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(hours=2),
}


def get_random_user() -> dict:
    """
    Получение данных по рандомному пользователю через API
    """

    url = 'https://randomuser.me/api/'
    try:
        logging.info('Попытка получения данных по пользователю')
        resp = requests.get(url).json()['results'][0]
        return resp
    except Exception as exception:
        logging.warning(f'{DAG_ID} Не удалось получить данные по пользователю')
        raise exception


def write_temp_data() -> None:
    """
    Запись данных во временную таблицу 'tmp_random_user'
    """

    data_user = get_random_user()
    new_df = pd.json_normalize(data_user, sep='_')

    try:
        pg_hook = PostgresHook('pgsql_db')
        new_df.to_sql(
            name='tmp_random_user',
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
    schedule_interval='*/5 * * * *',
    default_args=args,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    drop_tmp_random_user = PostgresOperator(
        task_id='drop_tmp_random_user',
        postgres_conn_id=CONN_ID,
        sql='''
            DROP TABLE IF EXISTS public.tmp_random_user
            '''
    )
    drop_tmp_random_user.doc_md = 'Удаление временной таблицы "tmp_random_user", если она существует'

    write_temp_data = PythonOperator(
        task_id='write_temp_data',
        python_callable=write_temp_data,
    )

    insert_new_values = PostgresOperator(
        task_id='insert_new_values',
        postgres_conn_id=CONN_ID,
        sql='''
            INSERT INTO public.random_user
            (
               gender,
               email,
               phone,
               cell,
               nat,
               name_title,
               name_first,
               name_last,
               location_street_number,
               location_street_name,
               location_city,
               location_state,
               location_country,
               location_postcode,
               location_coordinates_latitude,
               location_coordinates_longitude,
               location_timezone_offset,
               location_timezone_description,
               login_uuid,
               login_username,
               login_password,
               login_salt,
               login_md5,
               login_sha1,
               login_sha256,
               dob_date,
               dob_age,
               registered_date,
               registered_age,
               id_name,
               id_value,
               picture_large,
               picture_medium,
               picture_thumbnail              
            )
            SELECT
                gender,
                email,
                phone,
                cell,
                nat,
                name_title::varchar(3),
                name_first::varchar(40),
                name_last::varchar(40),
                location_street_number,
                location_street_name,
                location_city,
                location_state,
                location_country,
                location_postcode,
                location_coordinates_latitude,
                location_coordinates_longitude,
                location_timezone_offset,
                location_timezone_description,
                login_uuid::uuid,
                login_username,
                login_password,
                login_salt,
                login_md5,
                login_sha1,
                login_sha256,
                dob_date::timestamptz,
                dob_age,
                registered_date::timestamptz,
                registered_age,
                id_name,
                id_value,
                picture_large,
                picture_medium,
                picture_thumbnail
            FROM 
                public.tmp_random_user
            ON CONFLICT (login_uuid) DO NOTHING                
            '''
    )
    insert_new_values.doc_md = 'Вставка данных в таблицу "random_user"'

    drop_tmp_random_user_end = PostgresOperator(
        task_id='drop_tmp_random_user_end',
        postgres_conn_id='pgsql_db',
        sql='''
            DROP TABLE IF EXISTS public.tmp_random_user
            '''
    )
    drop_tmp_random_user_end.doc_md = 'Удаление временной таблицы "tmp_random_user", если она существует'

    drop_tmp_random_user >> write_temp_data >> insert_new_values >> drop_tmp_random_user_end
