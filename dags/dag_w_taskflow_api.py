from airflow.decorators import dag, task

from datetime import datetime, timedelta


default_args={
    'owner': 'bunny',
    'retries': 4,
    'retry_delay': timedelta(minutes=4)
}

@dag(
    dag_id='dag_w_taskflow_api_v0',
    default_args=default_args,
    start_date=datetime(2023, 8, 17),
    schedule_interval='@daily'
)
def hello_world_etl():

    @task(multiple_outputs=True) # task decorator
    def get_name():
        return {
            'firstname': 'Alice',
            'lastname': 'Boby'
        }
    
    @task()
    def get_age():
        return 25
    
    @task()
    def greet(firstname, lastname, age):
        print(f'Hello, firstname is {firstname} lastname is {lastname} and age is {age}')

    name = get_name()
    age = get_age()
    greet(name['firstname'], name['lastname'], age)


greet_dag=hello_world_etl()

