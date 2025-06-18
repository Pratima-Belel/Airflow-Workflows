from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'pratima'
}

with DAG(
    dag_id='execute_multiple_tasks_bashScripts',
    description="A DAG with multiple tasks and dependencies",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Manual Trigger
    template_searchpath='/mnt/c/Users/HP/Documents/airflow/dags/bash_scripts', 
    tags=["template_searchpath", "scripts"]
) as dag:
    # Define tasks
    taskA = BashOperator(
        task_id='taskA',
        bash_command='taskA.sh'
    )
    taskB = BashOperator(
        task_id='taskB',
        bash_command='taskB.sh'
    )
    taskC = BashOperator(
        task_id='taskC',
        bash_command='taskC.sh'
    )
    taskD = BashOperator(
        task_id='taskD',
        bash_command='taskD.sh'
    )
    taskE = BashOperator(
        task_id='taskE',
        bash_command='taskE.sh'
    )
    taskF = BashOperator(
        task_id='taskF',
        bash_command='taskF.sh'
    )
    taskG = BashOperator(
        task_id='taskG',
        bash_command='taskG.sh'
    )

taskA >> taskB >> taskE
taskA >> taskC >> taskF
taskA >> taskD >> taskG