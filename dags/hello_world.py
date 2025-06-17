from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'pratima'
}

dag = DAG(
    dag_id='hello_world',
    description="A Simple Hello World DAG",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["Beginner"]
)

task = BashOperator(
    task_id='hello_world_task',
    bash_command='echo Hello, World!!!',
    dag=dag
)

dag.doc_md = """
### Hello World Task
This task prints "Hello, World!!!" to the console.
"""
task