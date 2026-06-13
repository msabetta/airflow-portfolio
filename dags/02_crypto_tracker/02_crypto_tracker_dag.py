from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = '/opt/airflow/include/data/crypto'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

TRACKED_COINS = ['bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot']

def fetch_crypto_prices(**kwargs):
    """Fetches current prices from CoinGecko public API (no API key required)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    ids = ','.join(TRACKED_COINS)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd,eur&include_24hr_change=true&include_market_cap=true"
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Flatten data into rows
    rows = []
    for coin, values in data.items():
        rows.append({
            'coin': coin,
            'price_usd': values.get('usd'),
            'price_eur': values.get('eur'),
            'change_24h': values.get('usd_24h_change'),
            'market_cap_usd': values.get('usd_market_cap'),
            'fetched_at': datetime.utcnow().isoformat(),
        })
    
    df = pd.DataFrame(rows)
    date_str = kwargs['logical_date'].strftime('%Y-%m-%d')
    output_path = os.path.join(DATA_DIR, f'crypto_prices_{date_str}.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved {len(rows)} crypto prices to {output_path}")
    kwargs['ti'].xcom_push(key='prices_path', value=output_path)

def analyze_crypto_trends(**kwargs):
    """Analyzes price trends and detects significant movements."""
    prices_path = kwargs['ti'].xcom_pull(key='prices_path', task_ids='fetch_crypto_prices')
    df = pd.read_csv(prices_path)
    
    print("\n📊 Crypto Market Summary:")
    print("-" * 70)
    for _, row in df.iterrows():
        change = row['change_24h']
        emoji = "🟢" if change > 0 else "🔴"
        alert = " ⚠️ VOLATILE!" if abs(change) > 5 else ""
        print(f"  {emoji} {row['coin']:12s} | ${row['price_usd']:>10,.2f} | 24h: {change:>+6.2f}%{alert}")
    print("-" * 70)

def append_to_historical(**kwargs):
    """Appends daily snapshot to a historical CSV for trend analysis."""
    prices_path = kwargs['ti'].xcom_pull(key='prices_path', task_ids='fetch_crypto_prices')
    df_today = pd.read_csv(prices_path)
    
    historical_path = os.path.join(DATA_DIR, 'crypto_historical.csv')
    if os.path.exists(historical_path):
        df_hist = pd.read_csv(historical_path)
        df_combined = pd.concat([df_hist, df_today], ignore_index=True)
    else:
        df_combined = df_today
    
    df_combined.to_csv(historical_path, index=False)
    print(f"Historical file updated. Total records: {len(df_combined)}")

with DAG(
    '02_crypto_tracker_pipeline',
    default_args=default_args,
    description='Cryptocurrency price tracker using CoinGecko API with trend analysis',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'crypto', 'timeseries', 'portfolio'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_crypto_prices',
        python_callable=fetch_crypto_prices,
    )

    task_analyze = PythonOperator(
        task_id='analyze_trends',
        python_callable=analyze_crypto_trends,
    )

    task_historical = PythonOperator(
        task_id='append_historical',
        python_callable=append_to_historical,
    )

    task_fetch >> [task_analyze, task_historical]
