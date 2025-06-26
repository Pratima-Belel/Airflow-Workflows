import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'pratima'
}

def read_csv():
    # path = '/mnt/c/Users/HP/Documents/airflow/dags/datasets/'
    # df = pd.read_csv(path + 'insurance.csv')
    # print (df)
    # return df.to_json()
    return pd.read_csv('/mnt/c/Users/HP/Documents/airflow/dags/datasets/insurance.csv').to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids = 'read_csv_file') #ti = task instance
    # df = read_json(json_data)
    df = pd.read_json(json_data).dropna()
    df.to_csv('/mnt/c/Users/HP/Documents/airflow/dags/outputs/cleaned_insurance.csv', index=False)
    return df.to_json()

def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_all_nulls')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({'age':'mean', 'bmi':'mean','charges':'mean'}).reset_index()
    smoker_df.to_csv('/mnt/c/Users/HP/Documents/airflow/dags/outputs/grouped_by_smoker.csv', index=False)

def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_all_nulls')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({'age': 'mean','bmi': 'mean','charges': 'mean'}).reset_index()
    region_df.to_csv('/mnt/c/Users/HP/Documents/airflow/dags/outputs/grouped_by_region.csv', index=False)


with DAG(
    dag_id = 'execute_data_transformation',
    description = 'A DAG to run python data pipeline',
    default_args = default_args,
    schedule_interval = '@once',
    start_date = days_ago(1),
    tags = ['Python', 'CSV','Pipeline']
) as dag:

    task_read_csv = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv
    ) 
    task_remove_nulls = PythonOperator(
        task_id = 'remove_all_nulls',
        python_callable = remove_null_values
    ) 
    groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )    
    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )
task_read_csv >> task_remove_nulls >> [groupby_smoker, groupby_region]