from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

default_args = {
    'owner' : 'pratima',
    'start_date' : days_ago(1)
}
# send a custom email body or subject
def success_alert(context): 
    ti = context['ti']
    result = ti.xcom_pull(task_ids='display_input')
    send_email(
        to=["pratima.belel@gmail.com"], 
        subject=f"DAG {context['dag'].dag_id} succeeded",
        html_content=f"Your DAG has completed successfully. </br></br><h3>Task output: {result}</h3>"
    )

with DAG(
    dag_id = 'email_on_success',
    default_args = default_args,
    schedule_interval = None,
    on_success_callback = success_alert,
    tags = ['Beginner', 'Bash', 'Email']
) as dag:

    display_message = BashOperator(
        task_id = 'display_input',
        bash_command = 'echo "Good Morning, have a good day!!!"'
    )

display_message