from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def discover_files_S3(**context):
    
    date = context["data_interval_end"].format("YYYY-MM-DD")

    s3 = S3Hook(aws_conn_id='aws_conn')
    keys = s3.list_keys(
        bucket_name='bucket', 
        prefix=f"top_100/raw/{date}/"
    ) or []
    
    logging.info(f"Получены ключи за {date} из S3") 
    context['ti'].xcom_push(key='s3_keys', value=keys)


def load_data_to_pg_stg(**context):

    date = context["data_interval_end"].format("YYYY-MM-DD")
    keys = context['ti'].xcom_pull(task_ids='discover_files_S3', key='s3_keys')

    s3_hook = S3Hook(aws_conn_id='aws_conn')
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    insert_query = """
        INSERT INTO stg.daily_raw_data (country, source_date, raw_payload, loaded_at, processed)
        VALUES (%s, %s, %s, %s, %s)
        """
    
    for key in keys:
        country = key.split('/')[-1].split('_')[0]
        obj = s3_hook.read_key(
            key=key, 
            bucket_name='bucket'
        )
        pg_hook.run(
            insert_query, 
            parameters = (
                country, 
                date,
                obj, 
                datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), 
                'False'
                )
        )
        logging.info(f"Данные для {country} за {date} загружены в Postgres stg") 


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='raw_from_s3_to_pg',
    description='Load raw data from S3 to Postgres stg schema',
    tags=['s3', 'postgres', 'api', 'stg'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60  # частота проверки
    )

    discover_files_S3 = PythonOperator(
        task_id='discover_files_S3',
        python_callable=discover_files_S3
    )

    load_data_to_pg_stg = PythonOperator(
        task_id='load_data_to_pg_stg',
        python_callable=load_data_to_pg_stg
    )

    end = EmptyOperator(
        task_id="end"
    )    

    start >> sensor_on_raw_layer >> discover_files_S3 >> load_data_to_pg_stg >> end