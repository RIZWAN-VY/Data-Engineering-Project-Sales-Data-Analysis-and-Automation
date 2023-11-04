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

# 5.connecting Hive and Spark and doing Analysis 
spark_hive_connection = SparkSession.builder \
                        .appName("sales_data_analysis_using_apache_spark_sql") \
                        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                        .enableHiveSupport() \
                        .getOrCreate()


# Function and Task for finding Sales Statistics :
def sales_statistics():
    data = spark_hive_connection.sql("SELECT * FROM default.sales_data")
    statistics = data.describe(["no_of_units", "price", "amount"])
    statistics.show()
    # Save the analyzed data in ORC format
    statistics.write.orc("/home/rizwan/Desktop/Sales_Data_Insight/Sales_Statistics_orc")

sales_statistics_task = PythonOperator(
    task_id = 'calculating_sales_statistics',
    python_callable = sales_statistics,
    dag=dag
)

# Top Product by Sales :
def top_product():
    product_sales = spark_hive_connection.sql("SELECT product,SUM(no_of_units) AS total_units,SUM(price) AS total_price,SUM(amount) AS product_sales FROM default.sales_data GROUP BY product ORDER BY product_sales DESC")
    product_sales.show()
    # Save the analyzed data in ORC format
    product_sales.write.orc("/home/rizwan/Desktop/Sales_Data_Insight/product_sales_orc")

top_product_task = PythonOperator(
    task_id = 'top_selling_product',
    python_callable = top_product,
    dag=dag
)

# Top product category by Sales :
def top_product_category():
    category_sales = spark_hive_connection.sql("SELECT Category,SUM(amount) AS total_sales FROM default.sales_data GROUP BY Category ORDER BY total_sales DESC")
    category_sales.show()
    # Save the analyzed data in ORC format
    category_sales.write.orc("/home/rizwan/Desktop/Sales_Data_Insight/category_sales_orc")

top_product_category_task = PythonOperator(
    task_id = 'top_selling_product_category',
    python_callable = top_product_category,
    dag=dag
)

# Top Sales Representitive :
def best_sales_rep():
    sale_rep_totalsales = spark_hive_connection.sql("SELECT sales_rep, SUM(amount) AS sale_rep_totalsales FROM default.sales_data GROUP BY sales_rep ORDER BY sale_rep_totalsales DESC")
    sale_rep_totalsales.show()
    # Save the analyzed data in ORC format
    sale_rep_totalsales.write.orc("/home/rizwan/Desktop/Sales_Data_Insight/sale_rep_totalsales_orc")

best_sales_rep_task = PythonOperator(
    task_id = 'top_performing_sales_rep',
    python_callable = best_sales_rep,
    dag=dag
)

# Sales by city :
def sales_by_city():
    sales_by_city = spark_hive_connection.sql("SELECT city, SUM(amount) AS totalsales_by_city FROM default.sales_data GROUP BY city ORDER BY totalsales_by_city DESC")
    sales_by_city.show()
    # Save the analyzed data in ORC format
    sales_by_city.write.orc("/home/rizwan/Desktop/Sales_Data_Insight/sales_by_city_orc")

sales_by_city_task = PythonOperator(
    task_id = 'top_sales_by_cities',
    python_callable = sales_by_city,
    dag=dag
)

# 6. upload data insight to HDFS
upload_data_insight_cmd_HDFS = 'hadoop fs -put /home/rizwan/Desktop/Sales_Data_Insight /Sales_Data_Analysis_and_Automation'

upload_data_insight_HDFS_task = BashOperator(
    task_id = 'upload_data_insight_to_HDFS',
    bash_command = upload_data_insight_cmd_HDFS,
    dag = dag
)

# Task Dependencies :

create_folder_HDFS_task >> upload_data_HDFS_task >> hive_table_creation_task >> load_data_HDFS_to_Hive_task \
>> [sales_statistics_task, top_product_task, top_product_category_task, best_sales_rep_task,sales_by_city_task] >> upload_data_insight_HDFS_task
