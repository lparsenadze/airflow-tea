from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    greeting = ti.xcom_pull(task_ids='get_greeting', key='greeting')
    print(f'{first_name} {last_name} says "{greeting}" through the python greet function!')


def get_name(ti):
    ti.xcom_push(key='first_name', value='Lia')
    ti.xcom_push(key='last_name', value='Parsenadze')


def get_greeting(ti):
    ti.xcom_push(key='greeting', value="Bye y'all:)")

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_operator_v5',
    description='First DAG with python operator',
    start_date=datetime(2023, 7, 7),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        #op_kwargs={'greeting': "Hey y'all."}
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id='get_greeting',
        python_callable=get_greeting
    )

    [task2, task3] >> task1