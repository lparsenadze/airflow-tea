from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Doklad was prepared using this tutorial https://www.youtube.com/watch?v=K9AnJ9_ZAXE
# and airflow documentation

# Topics for today:
# What is Airflow (Work Scheduler, task manager)
# What is DAG?
# What is Task (DAG node)
# What is Operator?


default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag_v1',
    default_args=default_args,
    description='This is the first DAG of the Airflow tutorial.',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world this is the first task'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo this is the second task, running after the first'
    )
    task1.set_downstream(task2)