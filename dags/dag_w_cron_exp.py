from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'bunny',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_w_cron_expression_v3',
    default_args=default_args,
    description='example',
    start_date=datetime(2023, 8, 1),
    schedule_interval='0 3 * * Tue-Fri'
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo dag with cron expression'
    )