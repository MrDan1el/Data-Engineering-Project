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


def get_data_from_api(country):
    
    url = 'https://ws.audioscrobbler.com/2.0/'
    headers = {'user-agent': 'username'}
    payload = {
        'api_key': Variable.get("api_key"),
        'format': 'json',
        'method': 'geo.getTopTracks',
        'country': country,
        'limit': 100
    }
    
    try:
        logging.info(f"Попытка получения данных по API реквесту.")
        response = requests.get(url, headers=headers, params=payload)
        return response.json()
    except Exception as er:
        logging.error(f'Ошибка при попытке получения данных по API реквесту. {er}')


def extract_data_to_s3():
    
    country = 'Russian Federation'
    data = get_data_from_api(country)
    logging.info(f"Данные из API получены успешно.")
    
    timestamp = datetime.now().strftime('%Y-%m-%d')
    filename = f"raw/{timestamp}/{country}_{timestamp}.json"
    
    try:
        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_string(
            string_data=json.dumps(data, indent=4),
            key=filename,
            bucket_name='bucket',
            replace=True
        )
        logging.info(f"Данные загружены в S3 успешно.")
    except Exception as er:
        logging.error(f'Ошибка при загрузке данных в S3. {er}')


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

    extract_data_to_s3 = PythonOperator(
        task_id='extract_data_to_s3',
        python_callable=extract_data_to_s3
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> extract_data_to_s3 >> end