# -*- coding: utf-8 -*-
# Copyright (c) 2019 AxiataADA
"""
This DAG will aggregation raw data daily and store in our data lake (S3).
"""

import datetime, time
from datetime import date, timedelta

import calendar
import sys, os
import json
from pathlib import Path
import yaml
import ast
import boto3

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.sagemaker_training import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator
from airflow.providers.amazon.aws.operators.sagemaker_model import SageMakerModelOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# from operators.ms_teams_webhook_operator import MSTeamsWebhookOperator

#=================================================================================================================================#
#                                                Configs                                                                          #
#=================================================================================================================================#
if os.environ['AIRFLOW_ENV_NAME'] == 'ml-ops-prod':
    ENV = 'prod'
elif os.environ['AIRFLOW_ENV_NAME'] == 'ml-ops-preprod':
    ENV = 'preprod'
elif os.environ['AIRFLOW_ENV_NAME'] == 'staging':
    ENV = 'prod'
else:
    ENV = 'dev'

CONFIG_FILE = 'config_{}.yaml'.format(ENV)

# Load the DAG configuration from defined configuration file
path = Path(__file__).with_name(CONFIG_FILE)
with path.open() as f:
    config = yaml.safe_load(f)
dag_config = json.loads(json.dumps(config['dag'], default=str))

# Function
MS_TEAM_CONN = dag_config['default_args']['http_conn_id']

# def on_failure(context):
#     dag_id = context['dag_run'].dag_id
#     execution_date = context['ds_nodash']
#     task_id = context['task_instance'].task_id
    
#     teams_notification = MSTeamsWebhookOperator(
#         task_id="msteams_notify_failure", trigger_rule="all_done",
#         message = "`{}[MWAA]` - `{}` has failed on task: `{}` on `{}`".format(ENV, dag_id, task_id, execution_date),
#         theme_color="FF0000", http_conn_id=MS_TEAM_CONN)
#     teams_notification.execute(context)


def subtract_month(year, month, how_many):
    '''
    how_many - subtract by how_many months.
    '''
    assert (month > 0) & (month <=12), 'month is out of range - %d is passed'%month
    
    month = month - how_many
    while month < 1:
        month = month + 12
        year = year - 1
    return year, month


#demographic labels can be generated with cuurent month-3
#thus, traing data can be generated for that month with features spanning form m-4 backwards
# because, we collect features for upto a given month and merge that with the chun labels with the next month
current_month = datetime.datetime.now().month
current_year  = datetime.datetime.now().year

LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH           = subtract_month(current_year, current_month, 1)
TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH             = subtract_month(current_year, current_month, 1)
PREDICTION_DATA_GENERATION_YEAR, PREDICTION_DATA_GENERATION_MONTH   = subtract_month(current_year, current_month, 1) #given that the automation wll run in early of the month

RUN_MONTH_ID = "{{{{ macros.ds_format(macros.ds_add(ds, -{0}),'%Y-%m-%d','%Y%m') }}}}".format(str(30))
RUN_YEAR     = "{{{{ macros.ds_format(macros.ds_add(ds, -{0}),'%Y-%m-%d','%Y') }}}}".format(str(30))
RUN_MONTH    = "{{{{ macros.ds_format(macros.ds_add(ds, -{0}),'%Y-%m-%d','%m') }}}}".format(str(30))

COUNTRIES = config['country']

# EMR related
EMR_RELEASE     = config['emr_release']
EMR_LOG_PATH    = config['emr_log_path']
EMR_PACKAGES    = config['emr_packages']
EMR_BOOTSTRAP   = ast.literal_eval(config['emr_bootstrap'])
EMR_TAG         = ast.literal_eval(config['emr_tag'])
JobFlowRole = config['JobFlowRole']
ServiceRole = config['ServiceRole']

# pyspark files
MYAPPLABEL          = '{}/my_accurate_app_update_with_raw_transform.py'.format(config['S3_PATH_CODE'])
MODELV1             = '{}/demo_model.py'.format(config['S3_PATH_CODE'])
MODELV2             = '{}/demo_model_v2.py'.format(config['S3_PATH_CODE'])
LABELGEN            = '{}/accurate_app_raw_with_raw_transform.py'.format(config['S3_PATH_CODE'])
BASE_LABEL_PATH      = config['path_data']['Base_Label_data_path']
LABEL_NOT_MY_DATA_PATH   = config['path_data']['label_NOT_MY_data_path']
APP_REF_DATA    = config['path_data']['App_Ref_Data']
MODEL_PATH      = config['path_data']['model_output_path']
OUTPUT_PATH     = config['path_data']['output_path_pred']
MODEL_PATH_HISTORY = config['path_data']['model_path_hist']
# Inference
PREDICTION_DATA_PATH     = config['inference']['data']['TRANSFORM_DATA_INPUT']
PREDICTION_DATA_OUT      = config['inference']['data']['TRANSFORM_DATA_OUTPUT']
content_type             = config['inference']['parameters']['content_type']
join_source              = config['inference']['parameters']['join_source']
split_type               = config['inference']['parameters']['split_type']
inference_instance_count = config['inference']['instance']['instance_count']
inference_instance_type  = config['inference']['instance']['instance_type']



############################################################################################################################################

default_args = {
    'owner': 'ml-model',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}


#=================================================================================================================================#
#                                                EMR pipeline                                                               #
#=================================================================================================================================#
with DAG(**dag_config) as dag:
    country = 'MY'
    country_size = 'large'
    # fix this take size from config
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    instance_type   = config[country_size]['instance_type']
    cluster_config  = config[country_size]['cluster_config']
    driver_memory   = config[country_size]['steps_config']['driver-memory']
    driver_core     = config[country_size]['steps_config']['driver-cores']
    executor_memory = config[country_size]['steps_config']['executor-memory']
    executor_core   = config[country_size]['steps_config']['executor-cores']
    num_executors   = config[country_size]['steps_config']['num-executors']
    #==============================================================================================#
    #                                   Demographic label generation for Malaysia                                     #
    #==============================================================================================#
    demographic_MY_label_generation_step = [
                    # Step 1 - data generation
                    {
                    'Name': 'demographic_label_generation_MY_{}{:02d}'.format( LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            '/usr/bin/spark-submit', 
                            '--deploy-mode','client',
                            '--packages', EMR_PACKAGES,
                            '--driver-memory', driver_memory,
                            '--driver-cores', str(driver_core),
                            '--num-executors' , str(num_executors),
                            '--executor-memory', executor_memory,
                            '--executor-cores' , str(executor_core),
                            '--conf' , 'spark.driver.maxResultSize=0',
                            '--conf','spark.yarn.maxAppAttempts=1',
                            '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                            # 's3://ada-dev/demographic_model/airflow/script/my_accurate_app_update_with_raw_transform.py',
                            '{}/my_accurate_app_update_with_raw_transform.py'.format(config['S3_PATH_CODE']),
                            '--label_path', str(BASE_LABEL_PATH),
                            '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                            # '--output_path', str(APP_REF_DATA) 
                            # '--base_selected_ifas_path', config['input_data']['Label_data_path']
                            ]
                        }
                    }
                ]


    demographic_MY_label_generation_job = {
        'Name': 'demographic_MY_label_generation_{}{:02d}'.format(LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
        'ReleaseLabel': EMR_RELEASE,
        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
        'LogUri': EMR_LOG_PATH,
        'Steps': demographic_MY_label_generation_step,
        'Instances': instance_type,
        'BootstrapActions': EMR_BOOTSTRAP,
        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
        'Configurations': cluster_config, 
        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
        # 'ServiceRole': 'EMR_AutomationServiceRole',
        'JobFlowRole': JobFlowRole,
        'ServiceRole': ServiceRole ,
        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
        'Tags': EMR_TAG
    }
    demographic_label_MY_cluster_creator = EmrCreateJobFlowOperator(
        task_id='demographic_label_generation_MY_{}{:02d}'.format(LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
        job_flow_overrides= demographic_MY_label_generation_job,
        #on_failure_callback=on_failure
    )
    demographic_label_MY_job_sensor = EmrJobFlowSensor(
        task_id='watch_label_generation_MY_{}{:02d}'.format(LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
        job_flow_id="{{{{ task_instance.xcom_pull('demographic_label_generation_MY_{}{:02d}', key='return_value') }}}}".format(LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
        #on_failure_callback=on_failure
    )
    for c in COUNTRIES:
        for country, country_size in c.items():
            if country =='MY':
                for model_type in ['age','gender']:
                    training_data_step = [
                        # Step 1 - data generation
                        {
                        'Name': 'demographic_training_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                '/usr/bin/spark-submit', 
                                '--deploy-mode','client',
                                '--packages', EMR_PACKAGES,
                                '--driver-memory', driver_memory,
                                '--driver-cores', str(driver_core),
                                '--num-executors' , str(num_executors),
                                '--executor-memory', executor_memory,
                                '--executor-cores' , str(executor_core),
                                '--conf' , 'spark.driver.maxResultSize=0',
                                '--conf','spark.yarn.maxAppAttempts=1',
                                '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                '{}/demo_model.py'.format(config['S3_PATH_CODE']),
                                '--market' , country,
                                '--model' , model_type,
                                '--train_mode', 'full',
                                '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                # '--feature_path', MODEL_PATH,
                                # '--output_path', OUTPUT_PATH
                                ]
                            }
                        }
                    ]
                    
                    training_data_job = {
                        'Name': 'demographic_training_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ReleaseLabel': EMR_RELEASE,
                        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                        'LogUri': EMR_LOG_PATH,
                        'Steps': training_data_step,
                        'Instances': instance_type,
                        'BootstrapActions': EMR_BOOTSTRAP,
                        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                        'Configurations': cluster_config, 
                        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                        # 'ServiceRole': 'EMR_AutomationServiceRole',
                        'JobFlowRole': 'EMR_EC2_ADARole',
                        'ServiceRole': 'EMR_ServiceRole',
                        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                        'Tags': EMR_TAG
                    }

                    training_data_cluster_creator = EmrCreateJobFlowOperator(
                        task_id='demographic_training_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_overrides=training_data_job,
                        #on_failure_callback=on_failure
                    )

                    training_job_sensor = EmrJobFlowSensor(
                        task_id='demographic_watch_training_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_id="{{{{ task_instance.xcom_pull('demographic_training_{}_{}_{}{:02d}', key='return_value') }}}}".format(country, model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        #on_failure_callback=on_failure
                    )

                    ## prediction
                    demographic_prediction_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/demo_model.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'score',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]
                    demographic_prediction_job = {
                        'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ReleaseLabel': EMR_RELEASE,
                        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                        'LogUri': EMR_LOG_PATH,
                        'Steps': demographic_prediction_step,
                        'Instances': instance_type,
                        'BootstrapActions': EMR_BOOTSTRAP,
                        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                        'Configurations': cluster_config, 
                        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                        # 'ServiceRole': 'EMR_AutomationServiceRole',
                        'JobFlowRole': 'EMR_EC2_ADARole',
                        'ServiceRole': 'EMR_ServiceRole',
                        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                        'Tags': EMR_TAG
                    }

                    prediction_data_cluster_creator = EmrCreateJobFlowOperator(
                        task_id='demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_overrides=demographic_prediction_job,
                        #on_failure_callback=on_failure
                    )

                    prediction_job_sensor = EmrJobFlowSensor(
                        task_id='demographic_prediction_watch_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_id="{{{{ task_instance.xcom_pull('demographic_prediction_{}_{}_{}{:02d}', key='return_value') }}}}".format(country, model_type,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        #on_failure_callback=on_failure
                    )

                    # if len(COUNTRIES) == 1:
                    start >> demographic_label_MY_cluster_creator >> demographic_label_MY_job_sensor >> training_data_cluster_creator>> training_job_sensor>> prediction_data_cluster_creator>> prediction_job_sensor>> end
                    continue
            elif country in ['SG','PH','TH','BD','ID']:
                # start_train  = DummyOperator(task_id='start_model_training_{}'.format(country))
                # start_pred   = DummyOperator(task_id='start_predictions_{}'.format(country))
                instance_type   = config[country_size]['instance_type']
                cluster_config  = config[country_size]['cluster_config']
                driver_memory   = config[country_size]['steps_config']['driver-memory']
                driver_core     = config[country_size]['steps_config']['driver-cores']
                executor_memory = config[country_size]['steps_config']['executor-memory']
                executor_core   = config[country_size]['steps_config']['executor-cores']
                num_executors   = config[country_size]['steps_config']['num-executors']
                
                # LABEL_NOT_MY_DATA_PATH      = '{}/{}/{}{:02d}'.format(config['input_data']['label_NOT_MY_data_path'], country, LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH)
                # APP_REF_PATH = '{}/{}{:02d}'.format(config['path_data']['App_Ref_Data'], PREDICTION_DATA_GENERATION_YEAR, PREDICTION_DATA_GENERATION_MONTH)
                # PREDICTION_DATA_PATH    = '{}/{}/{}{:02d}'.format(config['output_path_pred'], country, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH)
                # MODEL_PATH  = '{}/{}/{}{:02d}'.format(config['model_output_path'], country, PREDICTION_DATA_GENERATION_YEAR, PREDICTION_DATA_GENERATION_MONTH)
                #==============================================================================================#
                #                                   Demographic label generation for other countries                                     #
                #==============================================================================================#
                
                demographic_label_generation_step = [
                    # Step 1 - data generation
                    {
                    'Name': 'demographic_label_generation_{}_{}{:02d}'.format(country, LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            '/usr/bin/spark-submit', 
                            '--deploy-mode','client',
                            '--packages', EMR_PACKAGES,
                            '--driver-memory', driver_memory,
                            '--driver-cores', str(driver_core),
                            '--num-executors' , str(num_executors),
                            '--executor-memory', executor_memory,
                            '--executor-cores' , str(executor_core),
                            '--conf' , 'spark.driver.maxResultSize=0',
                            '--conf','spark.yarn.maxAppAttempts=1',
                            '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                            '{}/accurate_app_raw_with_raw_transform.py'.format(config['S3_PATH_CODE']),
                            '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                            # '--app_ref_path', str(APP_REF_DATA)+'agg/',
                            '--market', str(country),
                            # '--output_path', str(LABEL_NOT_MY_DATA_PATH) 
                            # '--base_selected_ifas_path', config['input_data']['Label_data_path']
                            ]
                        }
                    }
                ]


                demographic_label_generation_job = {
                    'Name': 'demographic_label_generation_{}_{}{:02d}'.format(country,LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    'ReleaseLabel': EMR_RELEASE,
                    "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                    'LogUri': EMR_LOG_PATH,
                    'Steps': demographic_label_generation_step,
                    'Instances': instance_type,
                    'BootstrapActions': EMR_BOOTSTRAP,
                    'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                    'Configurations': cluster_config, 
                    # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                    # 'ServiceRole': 'EMR_AutomationServiceRole',
                    'JobFlowRole': JobFlowRole,
                    'ServiceRole': ServiceRole,
                    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                    'Tags': EMR_TAG
                }
                demographic_label_cluster_creator = EmrCreateJobFlowOperator(
                    task_id='demographic_label_generation_{}_{}{:02d}'.format(country,LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    job_flow_overrides= demographic_label_generation_job,
                    #on_failure_callback=on_failure
                )
                demographic_label_job_sensor = EmrJobFlowSensor(
                    task_id='watch_label_generation_{}_{}{:02d}'.format(country,LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    job_flow_id="{{{{ task_instance.xcom_pull('demographic_label_generation_{}_{}{:02d}', key='return_value') }}}}".format(country, LABEL_GENERATION_YEAR, LABEL_GENERATION_MONTH),
                    #on_failure_callback=on_failure
                ) 
                    # start >> demographic_label_my_cluster_creator >> demographic_label_my_job_sensor >>demographic_label_cluster_creator >> demographic_label_job_sensor >> end
                #==============================================================================================#
                #                           Training data generation (train+test)                              #
                #==============================================================================================#
                # if country in ['MY','TH','ID']:
                for model_type in ['age','gender']:
                    if country in ['MY','TH','ID']:
                        training_data_generation_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_training_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/demo_model.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'full',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]
                        demographic_prediction_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/demo_model.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'score',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]
                    elif country in ['BD','PH','SG']:
                        training_data_generation_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_training_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/demo_model_v2.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'full',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]
                        demographic_prediction_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/demo_model_v2.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'score',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]


                    training_data_generation_job = {
                        'Name': 'demographic_training_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ReleaseLabel': EMR_RELEASE,
                        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                        'LogUri': EMR_LOG_PATH,
                        'Steps': training_data_generation_step,
                        'Instances': instance_type,
                        'BootstrapActions': EMR_BOOTSTRAP,
                        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                        'Configurations': cluster_config, 
                        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                        # 'ServiceRole': 'EMR_AutomationServiceRole',
                        'JobFlowRole': 'EMR_EC2_ADARole',
                        'ServiceRole': 'EMR_ServiceRole',
                        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                        'Tags': EMR_TAG
                    }

                    training_data_cluster_creator = EmrCreateJobFlowOperator(
                        task_id='demographic_training_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_overrides=training_data_generation_job,
                        #on_failure_callback=on_failure
                    )

                    training_job_sensor = EmrJobFlowSensor(
                        task_id='demographic_watch_training_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_id="{{{{ task_instance.xcom_pull('demographic_training_{}_{}_{}{:02d}', key='return_value') }}}}".format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        #on_failure_callback=on_failure
                    )


                    #==============================================================================================#
                    #                                   Demographic perdiction model                                      #
                    #==============================================================================================#
                    # demographic_prediction_step = [
                    #         # Step 1 - data generation
                    #         {
                    #         'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                    #         'ActionOnFailure': 'TERMINATE_CLUSTER',
                    #         'HadoopJarStep': {
                    #             'Jar': 'command-runner.jar',
                    #             'Args': [
                    #                 '/usr/bin/spark-submit', 
                    #                 '--deploy-mode','client',
                    #                 '--packages', EMR_PACKAGES,
                    #                 '--driver-memory', driver_memory,
                    #                 '--driver-cores', str(driver_core),
                    #                 '--num-executors' , str(num_executors),
                    #                 '--executor-memory', executor_memory,
                    #                 '--executor-cores' , str(executor_core),
                    #                 '--conf' , 'spark.driver.maxResultSize=0',
                    #                 '--conf','spark.yarn.maxAppAttempts=1',
                    #                 '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                    #                 '{}/demo_model.py'.format(config['S3_PATH_CODE']),
                    #                 '--market' , country,
                    #                 '--model' , model_type,
                    #                 '--train_mode', 'score',
                    #                 '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                    #                 '--feature_path', MODEL_PATH,
                    #                 '--output_path', OUTPUT_PATH
                    #                 ]
                    #             }
                    #         }
                    #     ]
                    demographic_prediction_job = {
                        'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ReleaseLabel': EMR_RELEASE,
                        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                        'LogUri': EMR_LOG_PATH,
                        'Steps': demographic_prediction_step,
                        'Instances': instance_type,
                        'BootstrapActions': EMR_BOOTSTRAP,
                        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                        'Configurations': cluster_config, 
                        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                        # 'ServiceRole': 'EMR_AutomationServiceRole',
                        'JobFlowRole': 'EMR_EC2_ADARole',
                        'ServiceRole': 'EMR_ServiceRole',
                        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                        'Tags': EMR_TAG
                    }

                    prediction_data_cluster_creator = EmrCreateJobFlowOperator(
                        task_id='demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_overrides=demographic_prediction_job,
                        #on_failure_callback=on_failure
                    )

                    prediction_job_sensor = EmrJobFlowSensor(
                        task_id='demographic_watch_prediction_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_id="{{{{ task_instance.xcom_pull('demographic_prediction_{}_{}_{}{:02d}', key='return_value') }}}}".format(country, model_type,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        #on_failure_callback=on_failure
                    )

                    start >> demographic_label_MY_cluster_creator >> demographic_label_MY_job_sensor >>demographic_label_cluster_creator >> demographic_label_job_sensor >>training_data_cluster_creator>>training_job_sensor>> prediction_data_cluster_creator>>prediction_job_sensor>>end
            else:
                for model_type in ['age','gender']:
                    demographic_prediction_step = [
                            # Step 1 - data generation
                            {
                            'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    '/usr/bin/spark-submit', 
                                    '--deploy-mode','client',
                                    '--packages', EMR_PACKAGES,
                                    '--driver-memory', driver_memory,
                                    '--driver-cores', str(driver_core),
                                    '--num-executors' , str(num_executors),
                                    '--executor-memory', executor_memory,
                                    '--executor-cores' , str(executor_core),
                                    '--conf' , 'spark.driver.maxResultSize=0',
                                    '--conf','spark.yarn.maxAppAttempts=1',
                                    '--py-files' , ','.join([MYAPPLABEL,LABELGEN,MODELV1,MODELV2]), 
                                    '{}/predictor_demo.py'.format(config['S3_PATH_CODE']),
                                    '--market' , country,
                                    '--model' , model_type,
                                    '--train_mode', 'score',
                                    '--data_month', str(LABEL_GENERATION_YEAR)+str(LABEL_GENERATION_MONTH).zfill(2),
                                    # '--feature_path', MODEL_PATH_HISTORY,
                                    # '--output_path', OUTPUT_PATH
                                    ]
                                }
                            }
                        ]
                    demographic_prediction_job = {
                        'Name': 'demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        'ReleaseLabel': EMR_RELEASE,
                        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
                        'LogUri': EMR_LOG_PATH,
                        'Steps': demographic_prediction_step,
                        'Instances': instance_type,
                        'BootstrapActions': EMR_BOOTSTRAP,
                        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
                        'Configurations': cluster_config, 
                        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
                        # 'ServiceRole': 'EMR_AutomationServiceRole',
                        'JobFlowRole': 'EMR_EC2_ADARole',
                        'ServiceRole': 'EMR_ServiceRole',
                        'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
                        'Tags': EMR_TAG
                    }

                    prediction_data_cluster_creator = EmrCreateJobFlowOperator(
                        task_id='demographic_prediction_{}_{}_{}{:02d}'.format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_overrides=demographic_prediction_job,
                        #on_failure_callback=on_failure
                    )

                    prediction_job_sensor = EmrJobFlowSensor(
                        task_id='demographic_watch_prediction_{}_{}_{}{:02d}'.format(country,model_type ,TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        job_flow_id="{{{{ task_instance.xcom_pull('demographic_prediction_{}_{}_{}{:02d}', key='return_value') }}}}".format(country,model_type, TRAIN_DATA_GENERATION_YEAR, TRAIN_DATA_GENERATION_MONTH),
                        #on_failure_callback=on_failure
                    )
                    start >> demographic_label_MY_cluster_creator >> demographic_label_MY_job_sensor >> prediction_data_cluster_creator>>prediction_job_sensor>>end
                    # continue