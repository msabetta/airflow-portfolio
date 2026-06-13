import os
import pandas as pd
from datetime import datetime

SILVER_DIR = '/opt/airflow/include/data/silver'
GOLD_DIR = '/opt/airflow/include/data/gold'

def aggregate_data(**kwargs):
    logical_date = kwargs.get('logical_date', datetime.now())
    date_str = logical_date.strftime('%Y-%m-%d')
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    
    print("Starting Gold Layer processing...")
    
    orders_path = os.path.join(SILVER_DIR, f'orders_{date_str}.parquet')
    if not os.path.exists(orders_path):
        print(f"No orders found for {date_str}. Skipping Gold layer.")
        return
        
    orders_df = pd.read_parquet(orders_path)
    products_df = pd.read_parquet(os.path.join(SILVER_DIR, 'products.parquet'))
    users_df = pd.read_parquet(os.path.join(SILVER_DIR, 'users.parquet'))
    
    # Calculate Total Amount
    orders_df['total_amount'] = orders_df['quantity'] * orders_df['unit_price']
    
    # Daily Sales Aggregation
    daily_sales = orders_df.groupby('status').agg(
        total_revenue=('total_amount', 'sum'),
        total_orders=('order_id', 'count')
    ).reset_index()
    daily_sales['date'] = date_str
    
    daily_sales.to_parquet(os.path.join(GOLD_DIR, f'daily_sales_{date_str}.parquet'), index=False)
    print(f"Gold Layer: Daily Sales aggregated. Revenue: {daily_sales['total_revenue'].sum()}")

    # Top Products Aggregation
    merged_df = pd.merge(orders_df, products_df, on='product_id', how='left')
    top_products = merged_df.groupby('category').agg(
        items_sold=('quantity', 'sum'),
        category_revenue=('total_amount', 'sum')
    ).reset_index().sort_values(by='category_revenue', ascending=False)
    
    top_products.to_parquet(os.path.join(GOLD_DIR, f'top_categories_{date_str}.parquet'), index=False)
    print("Gold Layer: Top Categories aggregated.")

if __name__ == "__main__":
    aggregate_data(logical_date=datetime.now())
