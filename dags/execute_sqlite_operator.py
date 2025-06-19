from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'pratima'
}

with DAG(
    dag_id='execute_sqlite_operator',
    description="A DAG to create a table in SQLite",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["Beginner", "SQLite"]
) as dag:
    create_table_task = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            create table if not exists users (
                id integer PRIMARY KEY AUTOINCREMENT,
                name varchar(100) NOT NULL,
                age integer NOT NULL,
                city varchar(100),
                is_active boolean DEFAULT TRUE,
                created_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
        """
    )
create_table_task