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

# 1. create a folder in HDFS
folder_creation_cmd_HDFS = "hadoop fs -mkdir /Sales_Data_Analysis_and_Automation"

create_folder_HDFS_task = BashOperator(
    task_id = 'create_folder_in_HDFS',
    bash_command = folder_creation_cmd_HDFS,
    dag = dag
)

# 2. upload the downloaded data to the folder created in HDFS
upload_data_cmd_HDFS = "hadoop fs -put /home/rizwan/Downloads/Sales_Data.csv /Sales_Data_Analysis_and_Automation"

upload_data_HDFS_task = BashOperator(
    task_id = 'upload_data_to_HDFS',
    bash_command = upload_data_cmd_HDFS,
    dag = dag
)

# 3. create a table in hive which is compactable with the data
hive_table_creation_cmd = """
hive -e "CREATE TABLE sales_data (
    dte STRING,
    product STRING,
    category STRING,
    sales_rep STRING,
    city STRING,
    no_of_units INT,
    price DOUBLE,
    amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ('skip.header.line.count'='1');"
"""

hive_table_creation_task = BashOperator(
    task_id = 'hive_table_creation',
    bash_command = hive_table_creation_cmd,
    dag = dag
)

# 4. upload the data from HDFS to Hive table 
load_data_HDFS_to_Hive_cmd = """
hive -e "LOAD DATA INPATH '/Sales_Data_Analysis_and_Automation/Sales_Data.csv' INTO TABLE sales_data;"
"""

load_data_HDFS_to_Hive_task = BashOperator(
    task_id ='load_data_from_HDFS_to_sales_data_table_Hive',
    bash_command = load_data_HDFS_to_Hive_cmd,
    dag = dag
)
