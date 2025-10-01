from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor


DATE = datetime.now().strftime('%Y-%m-%d')


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='from_ods_to_dds_pg',
    description='Insert data from ods to dds in Postgres',
    tags=['ods', 'dds', 'postgres', 'star'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    sensor_on_etl = ExternalTaskSensor(
        task_id="sensor_on_etl",
        external_dag_id="etl_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60  # частота проверки
    )

    insert_into_dim_country = SQLExecuteQueryOperator(
        task_id="insert_into_dim_country",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dds.dim_country (country_code) 
            SELECT DISTINCT country_code 
            FROM ods.daily_raw_data
            WHERE source_date = %(date)s
            ON CONFLICT (country_code) DO NOTHING
        ''',
        parameters={"date": DATE}
    )

    insert_into_dim_artist = SQLExecuteQueryOperator(
        task_id="insert_into_dim_artist",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dds.dim_artist (artist_name) 
            SELECT DISTINCT artist_name 
            FROM ods.daily_raw_data
            WHERE source_date = %(date)s
            ON CONFLICT (artist_name) DO NOTHING
        ''',
        parameters={"date": DATE}
    )

    insert_into_dim_song = SQLExecuteQueryOperator(
        task_id="insert_into_dim_song",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dds.dim_song (song_name, duration_sec) 
            SELECT DISTINCT song_name, duration_sec
            FROM ods.daily_raw_data
            WHERE source_date = %(date)s
            ON CONFLICT (song_name, duration_sec) DO NOTHING
        ''',
        parameters={"date": DATE}
    )

    insert_into_fact_daily_top_100 = SQLExecuteQueryOperator(
        task_id="insert_into_fact_daily_top_100",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dds.fact_daily_top_100 (date, country_id, song_id, artist_id, song_rank, listeners_count)
            SELECT drd.source_date,
                    dc.country_id,
                    ds.song_id,
                    da.artist_id,
                    drd.song_rank,
                    drd.listeners_count
            FROM ods.daily_raw_data drd
                JOIN dds.dim_artist da ON da.artist_name = drd.artist_name
                JOIN dds.dim_song ds ON ds.song_name = drd.song_name AND ds.duration_sec = drd.duration_sec 
                JOIN dds.dim_country dc ON dc.country_code = drd.country_code
            WHERE drd.source_date = %(date)s
            ON CONFLICT (date, country_id, song_rank) DO NOTHING
        ''',
        parameters={"date": DATE}
    )

    end = EmptyOperator(
        task_id="end"
    )    

    start >> sensor_on_etl >> insert_into_dim_country >> insert_into_dim_artist >> insert_into_dim_song >> insert_into_fact_daily_top_100 >> end