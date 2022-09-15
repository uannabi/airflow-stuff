import pymssql
import logging
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'aws',
    'depends_on_past': False,
    'start_date': datetime(2010, 2, 20),
    'provide_context': True
}

dag = DAG(
    'mssql_conn_example', default_args=default_args, schedule_interval=None)
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

# drop_db = MsSqlOperator(
#     task_id="drop_db",
#     sql="DROP DATABASE IF EXISTS testdb;",
#     mssql_conn_id="mssql_default",
#     autocommit=True,
#     dag=dag
# )

# create_db = MsSqlOperator(
#     task_id="create_db",
#     sql="create database testdb;",
#     mssql_conn_id="mssql_default",
#     autocommit=True,
#     dag=dag
# )

# create_table = MsSqlOperator(
#     task_id="create_table",
#     sql="CREATE TABLE testdb.dbo.pet (name VARCHAR(20), owner VARCHAR(20));",
#     mssql_conn_id="mssql_default",
#     autocommit=True,
#     dag=dag
# )

# insert_into_table = MsSqlOperator(
#     task_id="insert_into_table",
#     sql="INSERT INTO testdb.dbo.pet VALUES ('Olaf', 'Disney');",
#     mssql_conn_id="mssql_default",
#     autocommit=True,
#     dag=dag
# )


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
        cursor.execute("SELECT * from users ")
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

# Upload the file
task_upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={
        'filename': '/Users/dradecic/airflow/data/posts.json',
        'key': 'posts.json',
        'bucket_name': 'airflow-etl-fex'
    }
)

select_query