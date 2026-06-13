from datetime import datetime, timedelta
import os
import re
import random
import pandas as pd
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = '/opt/airflow/include/data/logs'
fake = Faker()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

HTTP_METHODS = ['GET', 'POST', 'PUT', 'DELETE']
STATUS_CODES = [200, 200, 200, 200, 201, 301, 400, 403, 404, 404, 500, 502, 503]
ENDPOINTS = ['/api/users', '/api/orders', '/api/products', '/api/auth/login', 
             '/api/auth/logout', '/api/health', '/api/search', '/static/main.js',
             '/static/style.css', '/api/checkout']

def generate_fake_logs(**kwargs):
    """Generates realistic web server access logs."""
    os.makedirs(DATA_DIR, exist_ok=True)
    logical_date = kwargs['logical_date']
    date_str = logical_date.strftime('%Y-%m-%d')
    
    logs = []
    num_entries = random.randint(5000, 15000)
    
    for _ in range(num_entries):
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        timestamp = f"{date_str} {hour:02d}:{minute:02d}:{second:02d}"
        
        method = random.choice(HTTP_METHODS)
        endpoint = random.choice(ENDPOINTS)
        status = random.choice(STATUS_CODES)
        response_time = round(random.uniform(0.01, 2.0), 3)
        
        # Simulate occasional spikes (anomalies)
        if random.random() < 0.02:
            response_time = round(random.uniform(5.0, 30.0), 3)
            status = random.choice([500, 502, 503])
        
        ip = fake.ipv4()
        user_agent = fake.user_agent()
        
        logs.append({
            'timestamp': timestamp,
            'ip': ip,
            'method': method,
            'endpoint': endpoint,
            'status_code': status,
            'response_time_s': response_time,
            'user_agent': user_agent,
        })
    
    df = pd.DataFrame(logs)
    output_path = os.path.join(DATA_DIR, f'access_log_{date_str}.csv')
    df.to_csv(output_path, index=False)
    print(f"Generated {num_entries} log entries for {date_str}")
    kwargs['ti'].xcom_push(key='log_path', value=output_path)

def parse_and_analyze(**kwargs):
    """Parses logs and computes key metrics."""
    log_path = kwargs['ti'].xcom_pull(key='log_path', task_ids='generate_logs')
    df = pd.read_csv(log_path)
    
    total = len(df)
    errors = len(df[df['status_code'] >= 400])
    error_rate = (errors / total) * 100
    avg_response = df['response_time_s'].mean()
    p95_response = df['response_time_s'].quantile(0.95)
    p99_response = df['response_time_s'].quantile(0.99)
    
    # Top endpoints by traffic
    top_endpoints = df['endpoint'].value_counts().head(5)
    
    # Top error endpoints
    error_df = df[df['status_code'] >= 500]
    top_error_endpoints = error_df['endpoint'].value_counts().head(5)
    
    print("\n📊 Log Analytics Report:")
    print("=" * 60)
    print(f"  Total Requests:      {total:,}")
    print(f"  Error Rate:          {error_rate:.2f}%")
    print(f"  Avg Response Time:   {avg_response:.3f}s")
    print(f"  P95 Response Time:   {p95_response:.3f}s")
    print(f"  P99 Response Time:   {p99_response:.3f}s")
    print("\n  Top Endpoints:")
    for ep, count in top_endpoints.items():
        print(f"    {ep:30s} | {count:>6,} hits")
    if not top_error_endpoints.empty:
        print("\n  ⚠️ Top Error Endpoints (5xx):")
        for ep, count in top_error_endpoints.items():
            print(f"    {ep:30s} | {count:>6,} errors")
    print("=" * 60)

def detect_anomalies(**kwargs):
    """Detects anomalous patterns: high error rates or slow responses."""
    log_path = kwargs['ti'].xcom_pull(key='log_path', task_ids='generate_logs')
    df = pd.read_csv(log_path)
    
    # Detect slow requests (> 5s)
    slow_requests = df[df['response_time_s'] > 5.0]
    
    # Detect error bursts (group by hour)
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    hourly_errors = df[df['status_code'] >= 500].groupby('hour').size()
    
    anomalies = []
    if len(slow_requests) > 0:
        anomalies.append(f"Found {len(slow_requests)} slow requests (>5s)")
    
    for hour, count in hourly_errors.items():
        if count > 50:
            anomalies.append(f"Error burst at hour {hour:02d}:00 - {count} server errors")
    
    if anomalies:
        print("\n🚨 ANOMALIES DETECTED:")
        for a in anomalies:
            print(f"  ⚠️  {a}")
    else:
        print("\n✅ No anomalies detected.")

with DAG(
    '05_log_analytics_pipeline',
    default_args=default_args,
    description='Simulated web server log generation, parsing, and anomaly detection',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['logs', 'analytics', 'anomaly', 'portfolio'],
) as dag:

    task_generate = PythonOperator(
        task_id='generate_logs',
        python_callable=generate_fake_logs,
    )

    task_analyze = PythonOperator(
        task_id='parse_and_analyze',
        python_callable=parse_and_analyze,
    )

    task_anomaly = PythonOperator(
        task_id='detect_anomalies',
        python_callable=detect_anomalies,
    )

    task_generate >> [task_analyze, task_anomaly]
