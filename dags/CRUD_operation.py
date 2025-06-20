from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'pratima'
}

with DAG(
    dag_id = "CRUD_operation_sqlite",
    description = "A DAG to perform CRUD operations on SQLite",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,  # Manual Trigger
    tags = ["Beginner", "CRUD", "SQLite"]
) as dag:
    create_table = SqliteOperator(
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

    insert_values_1 = SqliteOperator(
        task_id='insert_values_1',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            INSERT INTO users (name, age, city, is_active) VALUES
            ('Alice', 30, 'New York', TRUE),
            ('Bob', 25, 'Los Angeles', TRUE),
            ('Charlie', 35, 'Chicago', FALSE),
            ('Diana', 28, 'Houston', TRUE),
            ('Ethan', 40, 'Los Angeles', TRUE),
            ('Fiona', 22, 'Philadelphia', FALSE);
        """
    )

    insert_values_2 = SqliteOperator(
        task_id='insert_values_2',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            INSERT INTO users (name, age) VALUES
            ('George', 45),
            ('Hannah', 32),
            ('Ian', 29);
        """
    )

    update_values = SqliteOperator(
        task_id='update_values',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            UPDATE users SET city = 'LA' 
            WHERE city = 'Los Angeles' OR city IS NULL;
        """
    )

    delete_values = SqliteOperator(
        task_id='delete_values',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            DELETE FROM users 
            WHERE is_active = FALSE;
        """
    )

    display_results = SqliteOperator(
        task_id='display_results',
        sqlite_conn_id='my_sqlite_conn',
        sql = r"""
            SELECT * FROM users;
        """,
        do_xcom_push=True  # This will allow the results to be pushed to XCom
    )

create_table >> [insert_values_1, insert_values_2] >> update_values >> delete_values >> display_results
