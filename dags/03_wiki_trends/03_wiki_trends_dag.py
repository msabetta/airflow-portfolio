from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = '/opt/airflow/include/data/wiki'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Wikimedia REST API - Most viewed articles
WIKI_API_BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access"

def fetch_top_articles(**kwargs):
    """Fetches top viewed Wikipedia articles for the previous day."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    logical_date = kwargs['logical_date']
    year = logical_date.strftime('%Y')
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')
    
    url = f"{WIKI_API_BASE}/{year}/{month}/{day}"
    headers = {'User-Agent': 'AirflowPortfolio/1.0 (airflow-portfolio@example.com)'}
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    articles = data.get('items', [{}])[0].get('articles', [])
    
    rows = []
    for article in articles[:50]:  # Top 50
        rows.append({
            'rank': article.get('rank'),
            'article': article.get('article'),
            'views': article.get('views'),
            'date': f"{year}-{month}-{day}",
        })
    
    df = pd.DataFrame(rows)
    output_path = os.path.join(DATA_DIR, f'wiki_top_{year}{month}{day}.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved top {len(rows)} Wikipedia articles to {output_path}")
    kwargs['ti'].xcom_push(key='wiki_path', value=output_path)

def analyze_trends(**kwargs):
    """Analyzes trending topics and identifies spikes."""
    wiki_path = kwargs['ti'].xcom_pull(key='wiki_path', task_ids='fetch_top_articles')
    df = pd.read_csv(wiki_path)
    
    # Filter out common pages (Main_Page, Special:Search, etc.)
    noise_pages = ['Main_Page', 'Special:Search', '-', 'Wikipedia:Featured_pictures']
    df_filtered = df[~df['article'].isin(noise_pages)]
    
    print("\n📰 Wikipedia Trending Topics:")
    print("-" * 60)
    for _, row in df_filtered.head(20).iterrows():
        views_k = row['views'] / 1000
        print(f"  #{row['rank']:>3d} | {row['article'][:40]:40s} | {views_k:>8.1f}K views")
    print("-" * 60)

def build_historical_trends(**kwargs):
    """Builds a historical trends file for downstream analysis."""
    wiki_path = kwargs['ti'].xcom_pull(key='wiki_path', task_ids='fetch_top_articles')
    df_today = pd.read_csv(wiki_path)
    
    historical_path = os.path.join(DATA_DIR, 'wiki_historical.csv')
    if os.path.exists(historical_path):
        df_hist = pd.read_csv(historical_path)
        df_combined = pd.concat([df_hist, df_today], ignore_index=True)
    else:
        df_combined = df_today
    
    df_combined.to_csv(historical_path, index=False)
    print(f"Historical trends updated. Total records: {len(df_combined)}")

with DAG(
    '03_wiki_trends_pipeline',
    default_args=default_args,
    description='Wikipedia trending topics tracker using Wikimedia REST API',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'wikipedia', 'trends', 'portfolio'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_top_articles',
        python_callable=fetch_top_articles,
    )

    task_analyze = PythonOperator(
        task_id='analyze_trends',
        python_callable=analyze_trends,
    )

    task_historical = PythonOperator(
        task_id='build_historical',
        python_callable=build_historical_trends,
    )

    task_fetch >> [task_analyze, task_historical]
