from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(name, greeting):
    print(f'{name} says "{greeting}" through the python greet function!')


with DAG(
    default_args=default_args,
    dag_id='dag_with_python_operator_v3',
    description='First DAG with python operator',
    start_date=datetime(2023, 7, 7),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': "Lia", 'greeting': "Hey y'all."}
    )

    task1