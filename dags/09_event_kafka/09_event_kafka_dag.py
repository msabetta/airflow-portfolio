from datetime import datetime, timedelta
import os
import json
import random
import uuid
import pandas as pd
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = '/opt/airflow/include/data/events'
fake = Faker()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

EVENT_TYPES = [
    'page_view', 'page_view', 'page_view',
    'add_to_cart', 'add_to_cart',
    'remove_from_cart',
    'begin_checkout',
    'purchase',
    'search',
    'signup',
    'login',
]

def simulate_event_stream(**kwargs):
    """Simulates a Kafka-like event stream by generating batches of JSON events."""
    os.makedirs(DATA_DIR, exist_ok=True)
    logical_date = kwargs['logical_date']
    date_str = logical_date.strftime('%Y-%m-%d')
    
    events = []
    num_events = random.randint(10000, 30000)
    
    # Simulate user sessions
    active_users = [str(uuid.uuid4()) for _ in range(500)]
    
    for i in range(num_events):
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': random.choice(EVENT_TYPES),
            'user_id': random.choice(active_users),
            'session_id': str(uuid.uuid4())[:8],
            'timestamp': f"{date_str}T{hour:02d}:{minute:02d}:{second:02d}Z",
            'properties': {
                'page': random.choice(['/home', '/products', '/cart', '/checkout', '/search']),
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'country': fake.country_code(),
            }
        }
        
        if event['event_type'] == 'purchase':
            event['properties']['amount'] = round(random.uniform(10, 500), 2)
            event['properties']['currency'] = 'USD'
        
        events.append(event)
    
    # Save as JSON Lines (simulating Kafka topic output)
    output_path = os.path.join(DATA_DIR, f'events_{date_str}.jsonl')
    with open(output_path, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"Simulated {num_events} events for {date_str}")
    kwargs['ti'].xcom_push(key='events_path', value=output_path)

def consume_and_transform(**kwargs):
    """Simulates a Kafka consumer that reads and transforms events into analytics-ready format."""
    events_path = kwargs['ti'].xcom_pull(key='events_path', task_ids='simulate_event_stream')
    
    events = []
    with open(events_path, 'r') as f:
        for line in f:
            events.append(json.loads(line))
    
    df = pd.json_normalize(events)
    
    # Flatten and save as Parquet
    output_path = os.path.join(DATA_DIR, 'events_transformed.parquet')
    df.to_parquet(output_path, index=False)
    print(f"Transformed {len(df)} events to Parquet")
    kwargs['ti'].xcom_push(key='transformed_path', value=output_path)

def compute_event_metrics(**kwargs):
    """Computes real-time-like metrics from the event stream."""
    transformed_path = kwargs['ti'].xcom_pull(key='transformed_path', task_ids='consume_and_transform')
    df = pd.read_parquet(transformed_path)
    
    # Event type distribution
    event_counts = df['event_type'].value_counts()
    
    # Conversion funnel
    page_views = len(df[df['event_type'] == 'page_view'])
    add_to_cart = len(df[df['event_type'] == 'add_to_cart'])
    checkouts = len(df[df['event_type'] == 'begin_checkout'])
    purchases = len(df[df['event_type'] == 'purchase'])
    
    # Revenue
    purchase_df = df[df['event_type'] == 'purchase']
    total_revenue = purchase_df['properties.amount'].sum() if 'properties.amount' in purchase_df.columns else 0
    
    # Device breakdown
    device_counts = df['properties.device'].value_counts()
    
    print("\n📊 Event Stream Analytics:")
    print("=" * 60)
    print("  Event Distribution:")
    for event_type, count in event_counts.items():
        print(f"    {event_type:20s} | {count:>8,}")
    
    print("\n  🔄 Conversion Funnel:")
    print(f"    Page Views:     {page_views:>8,}")
    print(f"    Add to Cart:    {add_to_cart:>8,} ({add_to_cart/max(page_views,1)*100:.1f}%)")
    print(f"    Begin Checkout: {checkouts:>8,} ({checkouts/max(add_to_cart,1)*100:.1f}%)")
    print(f"    Purchases:      {purchases:>8,} ({purchases/max(checkouts,1)*100:.1f}%)")
    print(f"\n  💰 Total Revenue: ${total_revenue:,.2f}")
    
    print("\n  📱 Device Breakdown:")
    for device, count in device_counts.items():
        print(f"    {device:10s} | {count:>8,}")
    print("=" * 60)

with DAG(
    '09_event_kafka_pipeline',
    default_args=default_args,
    description='Simulated Kafka event stream processing with conversion funnel analytics',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'streaming', 'events', 'portfolio'],
) as dag:

    task_produce = PythonOperator(
        task_id='simulate_event_stream',
        python_callable=simulate_event_stream,
    )

    task_consume = PythonOperator(
        task_id='consume_and_transform',
        python_callable=consume_and_transform,
    )

    task_metrics = PythonOperator(
        task_id='compute_event_metrics',
        python_callable=compute_event_metrics,
    )

    task_produce >> task_consume >> task_metrics
