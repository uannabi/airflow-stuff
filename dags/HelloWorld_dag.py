from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def helloWorld():
    print('hello world')


with DAG(dag_id='HelloWorld_dag',
         start_date=datetime(2022, 8, 28),
         schedule_interval="@hourly",
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="hello_world",
        python_callable='helloWorld')

task1