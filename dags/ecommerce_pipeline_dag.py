from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from data_generator.ecommerce_generator import EcommerceDataGenerator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# GCP Configuration
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'your-gcp-project-id')
GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET', 'your-gcs-bucket-name')
BQ_DATASET = 'ecommerce_raw'

# Define the Dataset using a safe, universal URI format to avoid Google validation bugs
# This will trigger the downstream dbt DAG once this pipeline completes successfully
ecommerce_raw_dataset = Dataset(f"gcp://bigquery/{GCP_PROJECT_ID}/{BQ_DATASET}")

# Local Data Paths
DATA_DIR = '/opt/airflow/include/data'

def generate_daily_data(**kwargs):
    logical_date = kwargs['logical_date']
    
    generator = EcommerceDataGenerator(output_dir=DATA_DIR)
    
    # Generate or load Users
    users_path = os.path.join(DATA_DIR, 'users.csv')
    if not os.path.exists(users_path):
        users_df = generator.generate_users(num_users=1000)
    else:
        users_df = pd.read_csv(users_path)
        
    # Generate or load Products
    products_path = os.path.join(DATA_DIR, 'products.csv')
    if not os.path.exists(products_path):
        products_df = generator.generate_products(num_products=100)
    else:
        products_df = pd.read_csv(products_path)
        
    # Generate Daily Orders based on logical_date
    generator.generate_daily_orders(users_df, products_df, date=logical_date, num_orders=200)

with DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    description='E-Commerce ELT Pipeline: Faker -> local -> GCS -> BigQuery -> dbt',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'portfolio', 'elt'],
) as dag:

    # 1. Generate Data
    task_generate_data = PythonOperator(
        task_id='generate_daily_data',
        python_callable=generate_daily_data,
    )

    # 2. Upload to GCS
    task_upload_users_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_users_gcs',
        src=f'{DATA_DIR}/users.csv',
        dst='raw/users.csv',
        bucket=GCS_BUCKET,
    )

    task_upload_products_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_products_gcs',
        src=f'{DATA_DIR}/products.csv',
        dst='raw/products.csv',
        bucket=GCS_BUCKET,
    )

    task_upload_orders_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_orders_gcs',
        src=f'{DATA_DIR}/orders_{{ ds }}.csv',
        dst='raw/orders_{{ ds }}.csv',
        bucket=GCS_BUCKET,
    )

    # 3. Load to BigQuery
    task_load_users_bq = GCSToBigQueryOperator(
        task_id='load_users_bq',
        bucket=GCS_BUCKET,
        source_objects=['raw/users.csv'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.users',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
    )

    task_load_products_bq = GCSToBigQueryOperator(
        task_id='load_products_bq',
        bucket=GCS_BUCKET,
        source_objects=['raw/products.csv'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.products',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
    )

    task_load_orders_bq = GCSToBigQueryOperator(
        task_id='load_orders_bq',
        bucket=GCS_BUCKET,
        source_objects=['raw/orders_{{ ds }}.csv'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.orders_{{ ds_nodash }}',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
    )

    # 4. Notify Completion via Dataset
    task_load_complete = EmptyOperator(
        task_id='load_complete',
        outlets=[ecommerce_raw_dataset],
    )

    # Dependencies
    task_generate_data >> [task_upload_users_gcs, task_upload_products_gcs, task_upload_orders_gcs]
    
    task_upload_users_gcs >> task_load_users_bq
    task_upload_products_gcs >> task_load_products_bq
    task_upload_orders_gcs >> task_load_orders_bq
    
    [task_load_users_bq, task_load_products_bq, task_load_orders_bq] >> task_load_complete