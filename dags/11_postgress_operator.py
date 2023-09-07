from datetime import datetime, timedelta


from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
# Don't forget that you need to first do
# >>> docker-compose up -d --no-deps --build postgres

default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2023, 7, 1),
    schedule_interval='0 3 * * MON-FRI', ## cron reference -> https://crontab.guru
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id='delete_from_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            delete from dag_runs where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}';
        """
    )

    # find templates at https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.htmlhttps://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    task3 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ds}}', '{{dag.dag_id}}')
        """
    )

    task1 >> task2 >> task3