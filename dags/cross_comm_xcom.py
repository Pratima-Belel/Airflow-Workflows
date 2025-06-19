from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import time

def add_1(value):
    print(f"Value: {value+1}!")
    return value + 1

def multiple_100(ti):
    value = ti.xcom_pull(task_ids = 'add_1') #ti = task instance
    print(f"Value: {value}!")
    return value * 100

with DAG(
    dag_id = "cross_comm_xcom",
    description = "Paasing result of task to another via XCom",
    default_args = {'owner': 'pratima'},
    start_date = days_ago(1),
    schedule_interval = None,  # Manual Trigger
    tags = ["Beginner", "XCom", "Python"]
) as dag:
    task_Add = PythonOperator(
        task_id = 'add_1',
        python_callable = add_1,
        op_kwargs = {'value': 9}
    )
    task_Multiply = PythonOperator(
        task_id = 'multiple_100',
        python_callable = multiple_100
        # op_kwargs = {'value': '{{ ti.xcom_pull(task_ids="add_1") }}'}
    )
task_Add >> task_Multiply