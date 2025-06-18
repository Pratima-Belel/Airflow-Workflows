from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'pratima'
}

with DAG(
    dag_id='execute_tasks_upstream_downstream',
    description="A DAG to execute multiple tasks with upstream and downstream dependencies",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',  # Runs only once
    tags=["Beginner","Scheduled", "Upstream", "Downstream"]
) as dag:
    # Define tasks
    taskA = BashOperator(
        task_id='task_A',
        bash_command='echo Executing Task A'
    )
    taskB = BashOperator(
        task_id='task_B',
        bash_command='echo Executing Task B'
    )
    taskC = BashOperator(
        task_id='task_C',
        bash_command='echo Executing Task C'
    )
    taskD = BashOperator(
        task_id='task_D',
        bash_command='echo Executing Task D'
    )

taskA.set_downstream(taskB) # Task A runs before Task B
# taskB.set_downstream(taskC)   
taskC.set_upstream(taskD)  # Task C runs after Task B