from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator


defaualt_args = {
    'owner': 'pratima'
}

with DAG(
    dag_id = 'sch_hello_world',
    description = "A Scheduled Hello World DAG",
    default_args = defaualt_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',  # Runs daily
    tags = ["Beginner", "Scheduled"]
) as dag:
    task = BashOperator(
        task_id = 'sch_hello_world_task',
        bash_command = 'echo Hello, World. This is a scheduled task!',
    )

task