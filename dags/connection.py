import pandas as pd
import pymssql
import logging
import sys
import pyspark.sql
from airflow import DAG
from datetime import datetime
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from io import BytesIO
s3_hook = S3Hook(aws_conn_id="s3_conn", region_name="ap-southeast-1")

buket_name = "BUCKET_NAME"

default_args = {
    'owner': 'aws',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 20),
    'provide_context': True
}

dag = DAG(
    'mssql_conn_example', default_args=default_args, schedule_interval=None)


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)



spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


def select_pet(**kwargs):
    try:
        conn = pymssql.connect(
            server='prod-mysql-fex.cluster-cogcq2nkkra1.ap-southeast-3.rds.amazonaws.com',
            user='data_reader',
            password='password',
            database='exchange'
        )

        # Create a cursor from the connection
        cursor = conn.cursor()
        query = cursor.execute("SELECT * from users ")
        row = cursor.fetchone()
        users = pd.read_sql(query, con=conn)
        conn.close()
        df = spark.createDataFrame(users)

        return df

        if row:
            print(row)
    except:
        logging.error("Error when creating pymssql database connection: %s", sys.exc_info()[0])
        return print('no data')


select_query = PythonOperator(
    task_id='select_query',
    python_callable=select_pet,
    dag=dag,
)


def sendDataToS3(df):

    apple_df = df

    region = "eu-central-1"
    # print(apple_df)
    csv_buffer = BytesIO
    apple_df.to_csv(csv_buffer)

    s3_hook._upload_file_obj(file_obj=csv_buffer.getvalue(), key=f"apple_data_{datetime.now()}.csv",
                             bucket_name=bucket_name)
    # s3_resource.Object(bucket, f"apple_data_{datetime.now()}.csv").put(Body=csv_buffer.getvalue())


t3 = PythonOperator(
    task_id="UploadToS3",
    python_callable=sendDataToS3,
    dag=dag
)




# Upload the file


select_query
