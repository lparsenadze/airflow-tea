from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# Example 2 with 2 parallel tasks and bash operator
default_args = {
    'owner': 'airflow tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag_v2',
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

    task3 = BashOperator(
        task_id='third',
        bash_command='echo this is third task, running after the first'
    )


    task1.set_downstream(task2)
    task1.set_downstream(task3)

    # Alternative synthax (Downstream definition)
    # task1 >> task2
    # task1 >> task3
    # OR
    # task1 >> [task2, task3]