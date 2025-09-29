from datetime import timedelta
import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


COUNTRY = 'Russian Federation'


def extract_data_from_s3(**context):

    key = f"raw/{context['ds']}/{COUNTRY}_{context['ds']}.json"

    logging.info(f"Попытка получения файла из S3")
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    obj = s3_hook.read_key(
        key=key, 
        bucket_name='bucket'
    )
    data = json.loads(obj)

    context['ti'].xcom_push(key='data_from_s3', value=data)


def transform_data(**context):
    
    data = context['ti'].xcom_pull(task_ids='extract_data_from_s3', key='data_from_s3')

    logging.info(f"Процесс трансформации данных")
    transformed_data = []
    tracks_list = data['tracks']['track']
    for track in tracks_list:
        track_info = {
            'name': track['name'],
            'duration': int(track['duration']),
            'listeners': int(track['listeners']),
            'artist': track['artist']['name'],
            'rank': int(track['@attr']['rank']),
            'date': context['ds'],
            'country': COUNTRY
        }
        transformed_data.append(track_info)    

    context['ti'].xcom_push(key='transformed_data', value=transformed_data)


def load_data_to_pg(**context):
    
    data = context['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

    logging.info(f"Попытка загрузки данных в Postgres")
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    insert_query = """
    INSERT INTO stg.daily_raw_data (song_name, artist_name, duration_sec, listeners_count, song_rank, source_date, country_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for track in data:
        pg_hook.run(
            insert_query, 
            parameters = (track['name'], track['artist'], track['duration'], track['listeners'], track['rank'], track['date'], 'RU')
        )


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='etl_from_s3_to_pg',
    description='Extract, transform and load raw data from S3 to Postgres DWH',
    tags=['etl', 's3', 'postgres', 'api'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    extract_data_from_s3 = PythonOperator(
        task_id='extract_data_from_s3',
        python_callable=extract_data_from_s3
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_data_to_pg = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=load_data_to_pg
    )

    end = EmptyOperator(
        task_id="end",
    )    

    start >> sensor_on_raw_layer >> extract_data_from_s3 >> transform_data >> load_data_to_pg >> end