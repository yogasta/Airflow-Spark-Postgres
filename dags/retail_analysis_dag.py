from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'retail_analysis_with_spark',
    default_args=default_args,
    description='A DAG to perform retail data analysis using Spark',
    schedule=timedelta(days=7),
)

spark_task = SparkSubmitOperator(
    task_id='spark_retail_analysis',
    application='/spark-scripts/retail_analysis.py',
    conn_id='spark_main',
    dag=dag,
    packages='org.postgresql:postgresql:42.6.0',

)

spark_task