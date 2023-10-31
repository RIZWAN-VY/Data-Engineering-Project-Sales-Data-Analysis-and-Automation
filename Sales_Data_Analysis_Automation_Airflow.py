'''
    SALES DATA ANALYSIS USING APACHE SPARK SQL AND AUTOMATION USING APACHE AIRFLOW
---------------------------------------------------------------------------------------

create a folder in HDFS  >>  upload the downloaded data to the folder created in HDFS

>>  create a table in Hive >>  upload the data from HDFS to Hive table  

>>  connecting Hive and Spark and doing Analysis and saving it in ORC format  

>>  upload data insight to HDFS
'''

# Libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession


default_arg ={
    'owner':'Rizwan',
    'start_date':datetime(2023,10,24)
}

dag = DAG(
    'Sales_Data_Analysis_Automation',
    default_args = default_arg,
    description = 'Sales Data Analysis using Spark SQL and automation with airflow',
    schedule_interval = None,
    catchup = False
)
