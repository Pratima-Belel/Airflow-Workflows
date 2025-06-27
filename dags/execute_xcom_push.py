import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'pratima',
    'start_date' : days_ago(1)
}

DATASET_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/datasets/insurance.csv'
OUTPUT_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/XCom_Push_Output/{0}.csv'

def read_csv(ti):
    df = pd.read_csv(DATASET_PATH)
    return ti.xcom_push(key='my_csv', value=df.to_json())

def remove_nulls(ti):
    json_data = ti.xcom_pull(key='my_csv')
    df = pd.read_json(json_data).dropna()
    df.to_csv(OUTPUT_PATH.format('cleaned_data'), index=False)
    return ti.xcom_push(key='my_clean_csv', value = df.to_json())

def filter_region_northwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'northwest']
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)

with DAG (
    dag_id = 'execute_xcom_push',
    description = 'Using XCom push to pass the values between tasks instead of pull',
    default_args = default_args,
    schedule_interval = None,
    tags = ['Beginner', 'Python', 'XCom', 'Push']
) as dag:
    
    task_read_csv = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv
    )
    task_remove_nulls = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_nulls
    )
    task_filter_northwest = PythonOperator(
        task_id = 'region_filter_northwest',
        python_callable = filter_region_northwest
    )

task_read_csv >> task_remove_nulls >> task_filter_northwest