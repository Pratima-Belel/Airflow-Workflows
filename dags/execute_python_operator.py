from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

def greet():
    print("Hello, World!")

default_args = {
    'owner' : 'pratima',
}

with DAG(
    dag_id='execute_python_operator',
    description="A DAG with a Python operator",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Manual Trigger
    tags=["Python", "Beginner"]
) as dag:
    # Define tasks
    task = PythonOperator(
        task_id='Greet_Hello_World',
        python_callable = greet
    )

task