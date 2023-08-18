
from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args={
    'owner': 'bunny',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='dag_w_catchup_backfill_v2',
    default_args=default_args,
    description='This is first dag',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily',
    catchup=False,

) as dag:

    task1=BashOperator(
        task_id='task1',
        bash_command='echo this is simple command!!!'
    )