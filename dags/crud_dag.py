import pymssql
import logging
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'zun',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 20),
    'provide_context': True
}

dag = DAG(
    'mssql_conn_example', default_args=default_args, schedule_interval=None)

user_db = MsSqlOperator(
    task_id="user_db",
    sql="SELECT * FROM users;",
    mssql_conn_id="mysql_fx",
    autocommit=True,
    dag=dag
)

usersKyc_db = MsSqlOperator(
    task_id="crypto_db",
    sql="Select * from usersKyc",
    mssql_conn_id="mysql_fx",
    autocommit=True,
    dag=dag
)



def select_pet(**kwargs):
    try:
        conn = pymssql.connect(
            server='prod-mysql-fex.cluster-cogcq2nkkra1.ap-southeast-3.rds.amazonaws.com',
            user='data_reader',
            password='liTORTeRNICU',
            database='fasset_exchange'
        )

        # Create a cursor from the connection
        cursor = conn.cursor()
        cursor.execute("SELECT * from users")
        row = cursor.fetchone()

        if row:
            print(row)
    except:
        logging.error("Error when creating pymssql database connection: %s", sys.exc_info()[0])


select_query = PythonOperator(
    task_id='select_query',
    python_callable=select_pet,
    dag=dag,
)

user_db>>usersKyc_db>>select_query
