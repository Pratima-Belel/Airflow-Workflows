from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner' : 'pratima',
    'start_date' : days_ago(1)
}

@dag(
    dag_id = 'execute_dag_taskflow_api',
    description = 'Running dag with TaskFlow-Api',
    default_args = default_args,
    schedule_interval = '@once',
    tags = ['Beginner', 'Python', 'TaskFlow-API']
)
def execute_with_taskflow_api():

    @task
    def task_A():
        print('Executed taskA...')
    
    @task
    def task_B():
        print([i for i in range(1,10)])

    @task
    def task_C():
        print('Executed taskC...')

    @task
    def task_D():
        print('Executed taskD...')

    task_A() >> task_B() >> [task_C(), task_D()]

execute_with_taskflow_api()