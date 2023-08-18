from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'bunny',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='first_dag_V2',
    default_args=default_args,
    description='This is first dag',
    start_date=datetime(2023, 8, 14),
    schedule_interval='@daily'
) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello, This is Task1 being running'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Hello, Task 2 will be running after Task1'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo Hey, Task 3 will be running after Task1 at the same time as Task2!'
    )

    # Task dependency method 1
    # task1.set_downstream(

    # Task dependency method 2
    task1 >> task2
    task1 >> task3

    # Task dependency method 3
    # task1 >> [task2, task3]