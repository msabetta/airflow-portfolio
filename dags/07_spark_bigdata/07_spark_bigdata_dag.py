from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '07_spark_bigdata_pipeline',
    default_args=default_args,
    description='Apache Spark Big Data processing orchestrated by Airflow',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'bigdata', 'portfolio'],
) as dag:

    # We use a BashOperator to run the pyspark script. 
    # In a real cluster, this would be a SparkSubmitOperator pointing to a YARN/K8s cluster.
    # For a portfolio, running it locally inside the Airflow worker demonstrates the API perfectly.
    task_run_spark = BashOperator(
        task_id='run_pyspark_job',
        bash_command='python3 /opt/airflow/dags/07_spark_bigdata/scripts/spark_processor.py',
    )
