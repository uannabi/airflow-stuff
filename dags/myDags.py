import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from etl import *
s3_hook = S3Hook(aws_conn_id="s3_conn", region_name="ap-southeast-1")

buket_name = "airflow-etl-fex"
default_args = {
    'owner': 'aws',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 20),
    'provide_context': True
}

def etl():
    user_df= extract_user_to_df()
    tranfrom_df=transform_user_df()

dag = DAG(dag_id="etl_pipline",
          default_args=default_args,
          schedule_interval="@daily",
          catchup=False)

etl_task=PythonOperator(task_id="etl_task",
                        python_callable=etl,
                        dag=dag)

def sendDataToS3(df_current):

    apple_df = df_current

    region = "ap-southeast-1"
    # print(apple_df)
    csv_buffer = BytesIO
    apple_df.to_csv(csv_buffer)

    s3_hook._upload_file_obj(file_obj=csv_buffer.getvalue(), key=f"apple_data_{datetime.now()}.csv",
                             bucket_name=buket_name)
    # s3_resource.Object(bucket, f"apple_data_{datetime.now()}.csv").put(Body=csv_buffer.getvalue())


s3 = PythonOperator(
    task_id="UploadToS3",
    python_callable=sendDataToS3,
    dag=dag
)
etl()
etl_task>>s3