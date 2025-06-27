import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup #group related tasks within a DAG
from airflow.utils.edgemodifier import Label #Add labels to the edges b/w tasks in a DAG, info about dependencies

default_args = {
    'owner' : 'pratima',
    'start_date' : days_ago(1),
    'schedule_interval' : None
}

DATASET_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/datasets/insurance.csv'
OUTPUT_PATH = '/mnt/c/Users/HP/Documents/airflow/dags/taskgroup_edgelabel_output/{0}.csv'

def read_csv(ti):
    df = pd.read_csv(DATASET_PATH)
    return ti.xcom_push(key='my_csv', value=df.to_json())

def remove_null_values(ti):
    json_data = ti.xcom_pull(key='my_csv')
    df = pd.read_json(json_data).dropna()
    df.to_csv(OUTPUT_PATH.format('clean_data'), index=False)
    return ti.xcom_push(key='my_clean_csv', value=df.to_json())

def groupby_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({'age':'mean', 'bmi':'mean','charges':'mean'}).reset_index()
    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)

def groupby_region(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({'age': 'mean','bmi': 'mean','charges': 'mean'}).reset_index()
    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

def filter_region_northwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'northwest']
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)

with DAG(
    dag_id = 'execute_taskgroup_edgelabel',
    description = 'A DAG to group tasks and label the edges of python data pipeline',
    default_args = default_args,
    tags = ['Python','Pipeline', 'TaskGroup', 'Label']
) as dag:

    with TaskGroup('read_and_preprocess') as read_and_preprocess: 

        task_read_csv = PythonOperator(
            task_id = 'read_csv_file',
            python_callable = read_csv
        ) 
        task_remove_nulls = PythonOperator(
            task_id = 'remove_all_nulls',
            python_callable = remove_null_values
        ) 
    task_read_csv >> task_remove_nulls  

    with TaskGroup('grouping_action') as grouping_action:    
        groupby_smoker = PythonOperator(
            task_id='groupby_smoker',
            python_callable=groupby_smoker
        )    
        groupby_region = PythonOperator(
            task_id='groupby_region',
            python_callable=groupby_region
        )
    with TaskGroup('filter_action') as filter_action: 
        task_filter_northwest = PythonOperator(
            task_id = 'region_filter_northwest',
            python_callable = filter_region_northwest
        )
read_and_preprocess >> Label('preprocessed_data') >> [grouping_action, filter_action]

