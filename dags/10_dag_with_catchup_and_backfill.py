from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v01',
    default_args=default_args,
    start_date=datetime(2023, 7, 1),
    schedule_interval='@daily', #'03**Mon-Fri' ##you can also use a cron Expression example -> 03**Tue (run on Tuesdays, 3 am)
    catchup=True # helps run dags back in time from start date
) as dag:
    task1 = BashOperator(
        task_id='taks1',
        bash_command='echo This is a bash command'
    )

    task1
# backfill with docker container scheduler id (b440319fad5b)
# >>> docker exec -it b440319fad5b bash
# >>> airflow dags backfill -s 2023-07-01 -e 2023-07-09 dag_with_catchup_and_backfill_v01