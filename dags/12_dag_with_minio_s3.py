from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'airflow_tutorial',
    'retires': 5,
    'retry_delay': timedelta(minutes=5)
}

# reference for airflow providers https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/index.html
with DAG(
    dag_id='dag_with_minio_s3_v01',
    start_date=datetime(2023, 7, 1),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=60
    )
    task1