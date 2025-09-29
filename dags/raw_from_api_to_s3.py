from datetime import datetime, timedelta
import json
import requests
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


COUNTRY = 'Russian Federation'


def get_data_from_api(**context):

    url = 'https://ws.audioscrobbler.com/2.0/'
    headers = {'user-agent': 'username'}
    payload = {
        'api_key': Variable.get("api_key"),
        'format': 'json',
        'method': 'geo.getTopTracks',
        'country': COUNTRY,
        'limit': 100
    }

    try:
        logging.info(f"Попытка получения данных по API реквесту")
        response = requests.get(url, headers=headers, params=payload)
        data = response.json()
        context['ti'].xcom_push(key='data_from_api', value=data)
        logging.info(f"Данные получены по API реквесту успешно")

    except Exception as er:
        logging.error(f'Ошибка при попытке получения данных по API реквесту - {er}')


def load_data_to_s3(**context):

    filename = f"raw/{context['ds']}/{COUNTRY}_{context['ds']}.json"
    data = context['ti'].xcom_pull(task_ids='get_data_from_api', key='data_from_api')
    
    try:
        logging.info(f"Подключение к S3")
        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_string(
            string_data=json.dumps(data, indent=4),
            key=filename,
            bucket_name='bucket',
            replace=True
        )
        logging.info(f"Данные загружены в S3 успешно")

    except Exception as er:
        logging.error(f'Ошибка при загрузке данных в S3 - {er}')


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='raw_from_api_to_s3',
    description='Extract raw data from API to S3',
    tags=['raw', 's3', 'data lake', 'api'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_data_from_api
    )

    load_data_to_s3 = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_data_to_s3
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> get_data_from_api >> load_data_to_s3 >> end