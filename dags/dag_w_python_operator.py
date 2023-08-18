from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args={
    'owner': 'bunny',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def greeting(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')

    print(f'Hello, {first_name} {last_name}, Your age is {age}')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Alice')
    ti.xcom_push(key='last_name', value='Boby')

def get_age(ti):
    ti.xcom_push(key='age', value=22)


with DAG(
    dag_id='dag_py_operator_v03',
    default_args=default_args,
    description='python operate',
    start_date=datetime(2023, 8, 15),
    schedule_interval='@daily'
) as dag:

    task1=PythonOperator(
        task_id='greeting',
        python_callable=greeting,
        op_kwargs={'age': 25}
    )

    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1