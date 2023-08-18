from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args={
    'owner': 'bunny',
    'retries': 4,
    'retry_delay': timedelta(minutes=4)
}


# cron setting
with DAG(
    dag_id='dag_w_postgres_v1',
    default_args=default_args,
    description='example',
    start_date=datetime(2023, 8, 16),
    schedule_interval='0 0 * * *'
) as dag:

    task1=PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql='''
            CREATE TABLE IF NOT EXISTS pet (
                pet_id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                pet_type VARCHAR NOT NULL,
                birth_date DATE NOT NULL,
                OWNER VARCHAR NOT NULL
            );
        ''' 
    )

    task2=PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql='''
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (1, 'Alice', 'CAT', '{{ds}}', '{{dag.dag_id}}')
        '''
    )

    task1 >> task2