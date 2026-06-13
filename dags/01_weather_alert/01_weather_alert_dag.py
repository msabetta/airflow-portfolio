from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from utils.helpers import fetch_weather, check_extreme_weather

MONITORED_CITIES = ['London', 'New York', 'Tokyo', 'Rome', 'Sydney']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def fetch_all_cities(**kwargs):
    """Fetches weather for all monitored cities."""
    results = []
    for city in MONITORED_CITIES:
        try:
            data = fetch_weather(city)
            analysis = check_extreme_weather(data)
            results.append(analysis)
        except Exception as e:
            print(f"Error fetching {city}: {e}")
            results.append({'city': city, 'alerts': [], 'has_alert': False, 'error': str(e)})
    
    kwargs['ti'].xcom_push(key='weather_results', value=results)
    print(f"Fetched weather for {len(results)} cities.")

def decide_alert_path(**kwargs):
    """Branch: if any city has an alert, go to alert task; otherwise skip."""
    results = kwargs['ti'].xcom_pull(key='weather_results', task_ids='fetch_weather_data')
    has_any_alert = any(r.get('has_alert', False) for r in results)
    
    if has_any_alert:
        return 'send_alert'
    return 'no_alert'

def send_alert_notification(**kwargs):
    """Sends alert notification (simulated - in production use Slack/Email)."""
    results = kwargs['ti'].xcom_pull(key='weather_results', task_ids='fetch_weather_data')
    alerts = [a for r in results for a in r.get('alerts', [])]
    
    print("=" * 60)
    print("🚨 WEATHER ALERT NOTIFICATION 🚨")
    print("=" * 60)
    for alert in alerts:
        print(f"  ⚠️  {alert}")
    print("=" * 60)
    print(f"Total alerts: {len(alerts)}")
    # In production: send to Slack, Email, PagerDuty, etc.

def log_weather_summary(**kwargs):
    """Logs a summary of all weather data for auditing."""
    results = kwargs['ti'].xcom_pull(key='weather_results', task_ids='fetch_weather_data')
    print("\n📊 Weather Summary Report:")
    for r in results:
        status = "⚠️ ALERT" if r.get('has_alert') else "✅ OK"
        print(f"  {r.get('city', 'N/A'):15s} | {r.get('temperature', 'N/A'):>6}°C | {r.get('condition', 'N/A'):15s} | {status}")

with DAG(
    '01_weather_alert_pipeline',
    default_args=default_args,
    description='Weather monitoring with API extraction and conditional alerting (BranchPythonOperator)',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'weather', 'branching', 'portfolio'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_all_cities,
    )

    task_branch = BranchPythonOperator(
        task_id='check_for_alerts',
        python_callable=decide_alert_path,
    )

    task_alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert_notification,
    )

    task_no_alert = EmptyOperator(
        task_id='no_alert',
    )

    task_summary = PythonOperator(
        task_id='log_summary',
        python_callable=log_weather_summary,
        trigger_rule='none_failed_min_one_success',
    )

    task_fetch >> task_branch >> [task_alert, task_no_alert] >> task_summary
