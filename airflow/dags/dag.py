from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
import boto3
import configparser
import time
import logging

# AWS setups
config = configparser.ConfigParser()
config.read('/home/workspace/aws.cfg')
aws_access_key_id=config['Credentials']['aws_access_key_id']
aws_secret_access_key=config['Credentials']['aws_secret_access_key']
def_region=config['default']['region']
project_s3_bucket = config['default']['region']
emr_cluster_id=config['EMR']['cluster_id']
notebook_id=config['EMR']['notebook_id']
notebookRelativePath=config['EMR']['notebookRelativePath']


aws_session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key,
                                   region_name=def_region)
emr = aws_session.client('emr')



default_args = {
    'owner': 'kallibekk',
    'start_date': datetime(2021, 3, 30),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

# Executes notebook on EMR Cluster
def emr_execute_notebook(emr_client,
                         EditorId,
                         RelativePath,
                         cluster_id,
                         ServiceRole):
    """
    Executes notebook on EMR cluster.
    Logs execution status every 30 seconds.
    
    Input:
        emr_client - boto3 EMR client with AWS credentials;
        EditorId - Notebook Id;
        RelativePath - relative path to a notebook;
        cluster_id - ID of a running cluster;
        ServiceRole - service role name to execute a notebook.
    """
    start_resp = emr_client.start_notebook_execution(
    EditorId=EditorId,
    RelativePath=RelativePath,
    ExecutionEngine={'Id':cluster_id},
    ServiceRole=ServiceRole)
    
    status='START_PENDING'    
    while status!='FINISHED':
        if status in ('FAILING','FAILED','STOP_PENDING','STOPPING','STOPPED'):
            raise Exception("Failed")
        time.sleep(30)
        logging.info("Execution status: " + status)    
        status = emr_client.describe_notebook_execution(NotebookExecutionId=start_resp["NotebookExecutionId"])["NotebookExecution"]["Status"]
        
# schedule dag to run daily at 7am
dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Runs the whole ETL cycle',
          schedule_interval='0 7 * * *',
          template_searchpath = ['/home/workspace/airflow/']
        )

# task to download NYC collisions data using bash script
download_collisions_data=BashOperator(
    task_id="download_crash_data",
    dag=dag,
    bash_command="curl -o {{params.save_path}} --create-dirs {{params.url}}",
    params={'save_path':'raw_data/vehicle_crash_data/vehicle_crash_data.csv',
           'url':'https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD'})

# task to download weather data using bash script
download_weather_data=BashOperator(
    task_id="download_weather_data",
    dag=dag,
    bash_command="wget --directory-prefix={{params.local_dir_path}} {{params.ftp_link}}",
    params={'local_dir_path':'raw_data/weather_by_year/',
           'ftp_link':'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/202*'}
) 
  
# Task to upload downloaded files to S3
load_raw_data_to_s3=BashOperator(
    task_id="load_raw_data_to_s3",
    dag=dag,
    bash_command="aws s3 cp {{params.local_dir}} s3://{{params.s3_dir}} --recursive",
    params={'local_dir':'raw_data/', 's3_dir':'kallibek-data-engineering-capstone-proj/raw_data'}
)

# Delete local files that were previously downloaded
delete_local_data=BashOperator(
    task_id='delete_local_data',
    dag=dag,
    bash_command='rm -r raw_data'
)

# Execute ETL notebook on a running EMR cluster
notebook_execution = PythonOperator(
    task_id='spark_etl',
    dag=dag,
    python_callable=emr_execute_notebook,
    op_kwargs={'emr_client':emr,
               'EditorId':notebook_id,
               'RelativePath':notebookRelativePath,
               'cluster_id':emr_cluster_id,
               'ServiceRole':'EMR_Notebooks_DefaultRole'},
    provide_context=False
)
# Sequence of tasks
[download_collisions_data , download_weather_data] >> load_raw_data_to_s3 >>  delete_local_data >> notebook_execution