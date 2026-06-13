from datetime import datetime, timedelta
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# We need to make sure python finds our scripts
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from scripts.clean_data import clean_data
from scripts.aggregate_data import aggregate_data
from data_generator.ecommerce_generator import EcommerceDataGenerator

BRONZE_DIR = '/opt/airflow/include/data'

def bronze_layer_extract(**kwargs):
    logical_date = kwargs.get('logical_date', datetime.now())
    print("Starting Bronze Layer Extraction...")
    generator = EcommerceDataGenerator(output_dir=BRONZE_DIR)
    
    # We just run the daily orders generation, assuming users/products exist or will be created
    users_df = generator.generate_users(num_users=100) if not os.path.exists(os.path.join(BRONZE_DIR, 'users.csv')) else None
    products_df = generator.generate_products(num_products=50) if not os.path.exists(os.path.join(BRONZE_DIR, 'products.csv')) else None
    
    if users_df is None:
        import pandas as pd
        users_df = pd.read_csv(os.path.join(BRONZE_DIR, 'users.csv'))
    if products_df is None:
        import pandas as pd
        products_df = pd.read_csv(os.path.join(BRONZE_DIR, 'products.csv'))
        
    generator.generate_daily_orders(users_df, products_df, date=logical_date, num_orders=300)
    print("Bronze Layer Extraction completed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '04_ecommerce_medallion_pipeline',
    default_args=default_args,
    description='Pure Python Medallion Architecture (Bronze -> Silver -> Gold)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'pandas', 'portfolio'],
) as dag:

    task_bronze = PythonOperator(
        task_id='bronze_layer_extract',
        python_callable=bronze_layer_extract,
    )

    task_silver = PythonOperator(
        task_id='silver_layer_clean',
        python_callable=clean_data,
    )

    task_gold = PythonOperator(
        task_id='gold_layer_aggregate',
        python_callable=aggregate_data,
    )

    task_bronze >> task_silver >> task_gold
