from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    "owner": "airflow_tutorial",
    "retries": 5,
    "retry_delays": timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api_v1',
     default_args=default_args,
     start_date=datetime(2023, 7, 9),
     schedule_interval='@daily')
def hello_world_etl():
    @task()
    def get_name():
        return 'Lia'

    @task()
    def get_greeting():
        return "Hello y'all"

    @task()
    def greet(name, greeting):
        print(f"{name} says '{greeting}' through taskflow api!")

    # this will go to Xcom
    name = get_name()
    greeting = get_greeting()
    greet(name=name, greeting=greeting)

greet_dag = hello_world_etl()