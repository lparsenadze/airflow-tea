from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'airflow_tutorial',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# [START howto_task_group]
with DAG(default_args=default_args,
    dag_id='taskgroup_dag_v1',
    description='First DAG with python operator',
    start_date=datetime(2023, 7, 7),
    schedule_interval="@daily") as dag:

    start = DummyOperator(task_id="start")

    # [START howto_task_group_section_1]
    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
        task_3 = DummyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]
    # [END howto_task_group_section_1]

    # [START howto_task_group_section_2]
    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = DummyOperator(task_id="task_1")
        task_0 = DummyOperator(task_id="task_0")
        # [START howto_task_group_inner_section_2]
        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = DummyOperator(task_id="task_3")
            task_4 = DummyOperator(task_id="task_4")

            [task_2, task_3] >> task_4
        # [END howto_task_group_inner_section_2]
        task_1 << task_0
    # [END howto_task_group_section_2]

    end = DummyOperator(task_id='end')

    start >> section_1 >> section_2 >> end
# [END howto_tasssk_group]