from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    "owner": "airflow_tutorial",
    "retries": 5,
    "retry_delays": timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api_v2',
     default_args=default_args,
     start_date=datetime(2023, 7, 9),
     schedule_interval='@daily')
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Lia',
            'last_name': 'Parsenadze'
        }

    @task()
    def get_greeting():
        return "Hello y'all"

    @task()
    def greet(first_name, last_name, greeting):
        print(f"{first_name} {last_name} says '{greeting}' through taskflow api!")

    # this will go to Xcom
    name_dict = get_name()
    greeting = get_greeting()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], greeting=greeting)

greet_dag = hello_world_etl()