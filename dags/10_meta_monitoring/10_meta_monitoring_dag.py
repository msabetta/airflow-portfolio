from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.session import provide_session
from sqlalchemy import func

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

@provide_session
def check_dag_health(session=None, **kwargs):
    """Queries Airflow's metadata DB to check the health of all DAGs."""
    
    # Get all DAG runs from the last 24 hours
    cutoff = datetime.utcnow() - timedelta(hours=24)
    
    dag_runs = session.query(
        DagRun.dag_id,
        DagRun.state,
        func.count(DagRun.id).label('count')
    ).filter(
        DagRun.execution_date >= cutoff
    ).group_by(
        DagRun.dag_id,
        DagRun.state
    ).all()
    
    print("\n🏥 DAG Health Check (Last 24 Hours):")
    print("=" * 70)
    
    dag_stats = {}
    for dag_id, state, count in dag_runs:
        if dag_id not in dag_stats:
            dag_stats[dag_id] = {}
        dag_stats[dag_id][state] = count
    
    for dag_id, states in sorted(dag_stats.items()):
        success = states.get('success', 0)
        failed = states.get('failed', 0)
        running = states.get('running', 0)
        total = success + failed + running
        health = "✅ HEALTHY" if failed == 0 else "🔴 FAILING"
        print(f"  {dag_id:40s} | ✅{success} ❌{failed} ▶{running} | {health}")
    
    if not dag_stats:
        print("  No DAG runs found in the last 24 hours.")
    print("=" * 70)
    
    kwargs['ti'].xcom_push(key='dag_stats', value={k: {sk: sv for sk, sv in v.items()} for k, v in dag_stats.items()})

@provide_session
def check_task_durations(session=None, **kwargs):
    """Identifies tasks that are running longer than expected (SLA violations)."""
    cutoff = datetime.utcnow() - timedelta(hours=24)
    
    # Get completed task instances with their durations
    task_instances = session.query(
        TaskInstance.dag_id,
        TaskInstance.task_id,
        TaskInstance.duration,
        TaskInstance.state
    ).filter(
        TaskInstance.start_date >= cutoff,
        TaskInstance.duration.isnot(None)
    ).all()
    
    print("\n⏱️ Task Duration Analysis (Last 24 Hours):")
    print("=" * 70)
    
    slow_tasks = []
    for ti in task_instances:
        if ti.duration and ti.duration > 300:  # More than 5 minutes
            slow_tasks.append({
                'dag': ti.dag_id,
                'task': ti.task_id,
                'duration': ti.duration,
                'state': ti.state,
            })
    
    if slow_tasks:
        print("  ⚠️ Slow Tasks (>5 min):")
        for st in sorted(slow_tasks, key=lambda x: x['duration'], reverse=True)[:10]:
            mins = st['duration'] / 60
            print(f"    {st['dag']:30s} | {st['task']:20s} | {mins:.1f} min | {st['state']}")
    else:
        print("  ✅ All tasks completed within acceptable duration.")
    print("=" * 70)

@provide_session
def generate_health_report(session=None, **kwargs):
    """Generates a comprehensive health report."""
    cutoff = datetime.utcnow() - timedelta(hours=24)
    
    total_runs = session.query(func.count(DagRun.id)).filter(DagRun.execution_date >= cutoff).scalar()
    failed_runs = session.query(func.count(DagRun.id)).filter(
        DagRun.execution_date >= cutoff,
        DagRun.state == 'failed'
    ).scalar()
    success_runs = session.query(func.count(DagRun.id)).filter(
        DagRun.execution_date >= cutoff,
        DagRun.state == 'success'
    ).scalar()
    
    success_rate = (success_runs / max(total_runs, 1)) * 100
    
    print("\n📋 Airflow Platform Health Report")
    print("=" * 50)
    print(f"  Report Time:    {datetime.utcnow().isoformat()}")
    print(f"  Period:         Last 24 Hours")
    print(f"  Total DAG Runs: {total_runs}")
    print(f"  Successful:     {success_runs}")
    print(f"  Failed:         {failed_runs}")
    print(f"  Success Rate:   {success_rate:.1f}%")
    
    if success_rate < 90:
        print("\n  🚨 ALERT: Success rate below 90%! Investigation recommended.")
    elif success_rate < 95:
        print("\n  ⚠️ WARNING: Success rate below 95%. Monitor closely.")
    else:
        print("\n  ✅ Platform health is GOOD.")
    print("=" * 50)

with DAG(
    '10_meta_monitoring_pipeline',
    default_args=default_args,
    description='Self-monitoring DAG that checks health of all other DAGs and tasks',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'meta', 'ops', 'portfolio'],
) as dag:

    task_dag_health = PythonOperator(
        task_id='check_dag_health',
        python_callable=check_dag_health,
    )

    task_durations = PythonOperator(
        task_id='check_task_durations',
        python_callable=check_task_durations,
    )

    task_report = PythonOperator(
        task_id='generate_health_report',
        python_callable=generate_health_report,
    )

    [task_dag_health, task_durations] >> task_report
