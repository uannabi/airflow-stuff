import pyspark.sql
# from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow import DAG
from datetime import datetime
from random import randint
from airflow.operators.bash import BashOperator
import os, json, boto3, psutil, socket

with DAG("connection", start_date=datetime(2022, 8, 28),
         schedule_interval="@daily", catchup=False) as dag:
    connection_check = MySqlOperator(
        task_id='check_connection',
        mysql_conn_id='mysql_fx',
        dag=dag
    )

    connection_result = BashOperator(
        print("Connection Successful")
    )

    connection_check >> connection_result