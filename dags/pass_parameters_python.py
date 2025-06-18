from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Function to be executed by the PythonOperator
def greet_Name(name):
    print(f'Hello, {name}!')

def greet_Name_Country(name, country):
    print(f'Hello, {name} from {country}!')

default_args = {
    'owner': 'pratima'
}
with DAG(
    dag_id='pass_parameters_python',
    description="Python operator in DAGs with parameters",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Manual Trigger
    tags=["Parameters", "Python"]
) as dag:
    # Define tasks
    task1 = PythonOperator(
        task_id='greet_name',
        python_callable=greet_Name,
        op_kwargs={'name': 'Pratima'}
    )
    task2 = PythonOperator(
        task_id='greet_name_country',
        python_callable=greet_Name_Country,
        op_kwargs={'name': 'Harry', 'country': 'India'}
    )

# Set task dependencies
task1 >> task2