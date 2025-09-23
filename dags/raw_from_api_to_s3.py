from datetime import datetime, timedelta
import json
import requests
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


COUNTRY = 'Russian Federation'


def get_data_from_api():
    
    url = 'https://ws.audioscrobbler.com/2.0/'
    headers = {'user-agent': 'username'}
    payload = {
        'api_key': Variable.get("api_key"),
        'format': 'json',
        'method': 'geo.getTopTracks',
        'country': COUNTRY,
        'limit': 100
    }
    
    response = requests.get(url, headers=headers, params=payload)
    return response.json()


def extract_data_to_s3():
    
    data = get_data_from_api()
    
    # Генерируем имя файла
    timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    filename = f"lastfm_api/{COUNTRY}/{COUNTRY}_{timestamp}.json"
   
    # Используем S3Hook
    hook = S3Hook(aws_conn_id='aws_conn')
    hook.load_string(
        string_data=json.dumps(data, indent=4),
        key=filename,
        bucket_name='bucket',
        replace=True
    )


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='raw_from_api_to_s3',
    description='Extract and Load raw data from API to S3',
    tags=['raw', 's3', 'data lake', 'api'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    extract_data_to_s3 = PythonOperator(
        task_id='extract_data_to_s3',
        python_callable=extract_data_to_s3
    )

    extract_data_to_s3