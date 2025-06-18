from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Function to be executed by the PythonOperator
def taskA():
    print("Execute taskA")
def taskB():
    print("Execute taskB")
def taskC():
    print("Execute taskC")
def taskD():
    print("Execute taskD")

default_args = {
    'owner' : 'pratima'
}

with DAG(
    dag_id='multiple_python_tasks',
    description="A DAG with multiple Python tasks and dependencies",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Manual Trigger
    tags=["Python", "Multiple_Tasks"]
) as dag:
    taskA = PythonOperator(
        task_id='taskA',
        python_callable=taskA
    )
    taskB = PythonOperator(
        task_id='taskB',
        python_callable=taskB
    )
    taskC = PythonOperator(
        task_id='taskC',
        python_callable=taskC
    )
    taskD = PythonOperator(
        task_id='taskD',
        python_callable=taskD
    )

taskA >> [taskB, taskC]
[taskB, taskC] >> taskD