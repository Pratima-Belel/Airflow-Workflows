import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner' : 'pratima',
    'start_date' : days_ago(1)
}

DATASET_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/datasets/insurance.csv'

OUTPUT_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/outputs/output/{0}.csv'

def read_csv():
    return pd.read_csv(DATASET_PATH).to_json()

def remove_nulls(ti):
    json_data = ti.xcom_pull(task_ids= 'read_csv_file')
    df  = pd.read_json(json_data)
    df = df.dropna()
    return df.to_json()

def determine_branch():
    var = Variable.get("filter_by", default_var=None)
    if var.startswith('filter'):
        return var
    else:
        return 'groupby_region_smoker'

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'southwest']
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)

def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({'age': 'mean', 'bmi': 'mean','charges': 'mean'}).reset_index()
    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)
    smoker_df = df.groupby('smoker').agg({'age': 'mean', 'bmi': 'mean','charges': 'mean'}).reset_index()
    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)

with DAG(
    dag_id = 'execute_branch_variable',
    description = 'Perform filter using variable and branch condition',
    default_args = default_args,
    schedule_interval = '@once',
    tags = ['Branch','Variable','Filter','Python']
) as dag:

    read_csv = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv
    )
    remove_nulls = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_nulls
    )
    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',
        python_callable=filter_by_southwest
    )

    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )

read_csv >> remove_nulls >> determine_branch >> [filter_by_southwest, groupby_region_smoker]