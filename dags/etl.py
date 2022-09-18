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
import pyspark.sql.functions as F
import pyspark.sql


date='2022-09-18'
##create spark session
spark = pyspark.sql.SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

##read movies table from db using spark
def extract_user_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", "prod-mysql-fex.cluster-cogcq2nkkra1.ap-southeast-3.rds.amazonaws.com") \
        .option("dbtable", "users") \
        .option("user", "data_reader") \
        .option("password", "liTORTeRNICU") \
        .option("database", "fasset_exchange") \
        .load()
    return users_df

def transform_user_df(users_df):
    ## transforming tables
    split_col = F.split(users_df['createdAt'], ' ')
    df = users_df.withColumn('date', split_col.getItem(0))
    df1 = df.filter(df['date'] == date)
    df_current = df1.select('id', 'email', 'phone', 'isKycApproved', 'userType', 'date')
    return df_current





if __name__ == "__main__":
    extract_user_to_df()
    transform_user_df()

    # movies_df = extract_movies_to_df()
    # users_df = extract_users_to_df()
    ##pass the dataframes to the transformation function
    # ratings_df = transform_user_df(movies_df, users_df)
    ##load the ratings dataframe
    # load_df_to_db(ratings_df)

