import os
import pandas as pd
from datetime import datetime

# Paths
BRONZE_DIR = '/opt/airflow/include/data'
SILVER_DIR = '/opt/airflow/include/data/silver'

def clean_data(**kwargs):
    logical_date = kwargs.get('logical_date', datetime.now())
    date_str = logical_date.strftime('%Y-%m-%d')
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    
    print("Starting Silver Layer processing...")
    
    # Clean Users
    users_path = os.path.join(BRONZE_DIR, 'users.csv')
    if os.path.exists(users_path):
        users_df = pd.read_csv(users_path)
        users_df.drop_duplicates(subset=['user_id'], inplace=True)
        users_df['registration_date'] = pd.to_datetime(users_df['registration_date'])
        users_df.to_parquet(os.path.join(SILVER_DIR, 'users.parquet'), index=False)
        print(f"Cleaned users. Rows: {len(users_df)}")
        
    # Clean Products
    products_path = os.path.join(BRONZE_DIR, 'products.csv')
    if os.path.exists(products_path):
        products_df = pd.read_csv(products_path)
        products_df.drop_duplicates(subset=['product_id'], inplace=True)
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')
        products_df.to_parquet(os.path.join(SILVER_DIR, 'products.parquet'), index=False)
        print(f"Cleaned products. Rows: {len(products_df)}")
        
    # Clean Orders
    orders_path = os.path.join(BRONZE_DIR, f'orders_{date_str}.csv')
    if os.path.exists(orders_path):
        orders_df = pd.read_csv(orders_path)
        orders_df.drop_duplicates(subset=['order_id'], inplace=True)
        orders_df['order_timestamp'] = pd.to_datetime(orders_df['order_timestamp'])
        orders_df['unit_price'] = pd.to_numeric(orders_df['unit_price'], errors='coerce')
        orders_df['quantity'] = pd.to_numeric(orders_df['quantity'], errors='coerce')
        
        # Filter out negative quantities or prices
        orders_df = orders_df[(orders_df['quantity'] > 0) & (orders_df['unit_price'] > 0)]
        
        orders_df.to_parquet(os.path.join(SILVER_DIR, f'orders_{date_str}.parquet'), index=False)
        print(f"Cleaned orders for {date_str}. Rows: {len(orders_df)}")
    
    print("Silver Layer processing completed.")

if __name__ == "__main__":
    clean_data(logical_date=datetime.now())
