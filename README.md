# Udacity Data Engineering Nanodegree Capstone Project
# Topic: Data Engineering for Vehicle Collisions ans Weather Analytics.

by Kallibek Kazbekov

Date: 4/8/2021

---
# Project summary

## Overview

The code in this project builds a scheduled data engineering pipeline to prepare data for both descriptive and predictive analytics. An example of descriptive analytics could be a dashboard showing a count of accidents that happened yesterday. In the case of predictive analytics, extracted-transformed-loaded data might be used to understand the relationship between weather parameters and vehicle collisions to be able to prepare for severe weather events and associated road accidents. 

New York City was picked as a study. 

Tools used: Airflow, PySpark, S3.

## Data Collection and ETL

The data collection and ETL was chosen to be orchestrated by Airflow because it is an open-source tool that offers many flexibilities. In the project, Airflow triggers a code run every day at 7am. 

![Alt text](airflow_dag.png?raw=false "Airflow DAG")

# Data Collection

Weather data comes from Daily Global Historical Climatology Network (GHCN-DAILY) Version 3.26  (ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily). 

The second dataset is The Motor Vehicle Collisions which contains information from all police reported motor vehicle collisions in NYC (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).

Both datasets are updated on daily basis.

Data Collection consists of three stage:

1. Download datasets from original sources;
2. Upload to S3 project bucket;
3. Clean downloaded data from local machine.


# ETL on Spark cluster

All ETL processes are implemented in a jupyter notebook (spark_etl.ipynb). After data collection is complete Airflow submits a notebook execution task to a running EMR cluster using a python operator executing a boto3 command. While running the Python Operator logs status updates every 30 seconds.

## Step 1: Load data from S3

In this step, both datasets are extracted from S3 bucket to Spark. Also, data corresponding to the Central Park Weather Station (ID USW00094728) are filtered. 

## Step 2: Transform data

Here, Spark:

1. summarizes collisions table by date, summarize by count of collisions, sum of injured and killed;
2. makes a separate table called Time table by extracting datetime from collisions table and also extracting elements like year, day, hour from the datetime;
3. reshapes weather data to be in tidy format.

![Alt text](relational_diagram.png?raw=false "Data Model")


## Step 3: Join dataframes for analytics

During this step, daily summary of collisions and daily weather data are inner-joined to be able to analyze patterns between weather and collisions.

## Step 4: Load to S3

Finally, the Spark writes generated and transformed dataframes to parquet and csv files in S3 bucket. 

![Alt text](data_collection_and_etl.png?raw=false "Data collection and ETL")

# Project instructions on how to run the Python scripts

The project uses AWS CLI v2 and Boto3 v1.17.

# An explanation of the files in the repository

## `airflow/dags/dag.py`

The file defines the DAG.

`dag = DAG('dag_id', default_args=default_args,...)` - DAG initialization

```python
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
```

## `./spark_etl.ipynb` - contains code for all ETL steps.

Each dataframe in the ETL process is checked for quality (count of rows).

```python
def quality_check(df):
    """
    Prints five rows of a dataframe.
    Then, checks count of rows.
    If rows<1 raises an exception
    
    Input: df - pyspark DataFrame
    """
    row_count=df.count()
    print(df.limit(5).toPandas())
    if row_count<1:
        raise Exception("Datafrane has no rows")
    print('count of rows: ', row_count)    
```

## `aws.cfg`

The file contains AWS credentials (access key ID  and secret access key) and other details 
for programmatic access to AWS services.

[Credentials]
aws_access_key_id=''
aws_secret_access_key=''

[default]
region=''

[S3]
project_bucket = ''

[EMR]
cluster_id=''
notebook_id=''
notebookRelativePath=''
