from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from random import choice

default_args = {
    'owner' : 'pratima'
}

def has_license():
    return choice([True, False])

def branch(ti):
    # if ti.xcom_pull(task_ids='has_driving_license'):
    #     return 'is_eligible_to_drive'
    # else:
    #     return 'not_eligible_to_drive'
    return 'is_eligible_to_drive' if ti.xcom_pull(task_ids='has_driving_license') else 'not_eligible_to_drive'

def eligible_to_drive():
    print("You're eligible to drive.")

def not_eligible_to_drive():
    print("You're not eligible to drive, you don't have license")


with DAG(
    dag_id = 'execute_branch_operator',
    description = 'A DAG to perform conditional branching.',
    default_args = default_args,
    schedule_interval = '@once',
    start_date = days_ago(1),
    tags = ['Python', 'Branch','Beginner', 'Condition'] 
) as dag:

    task_has_license = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_license
    )
    task_branch = BranchPythonOperator(
        task_id = 'conditional_Branch',
        python_callable = branch
    )
    task_eligible = PythonOperator(
        task_id = 'is_eligible_to_drive',
        python_callable = eligible_to_drive
    )
    task_not_eligible = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive
    )

task_has_license >> task_branch >> [task_eligible, task_not_eligible]