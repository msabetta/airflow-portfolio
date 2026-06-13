from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import functions from our plugin
from ml_pipeline.train_model import prepare_data, train_and_log_model, evaluate_model

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Wrapper function for training to simulate hyperparameter changes
def run_training():
    """Runs the training with slightly randomized hyperparameters for demonstration."""
    # We randomize hyperparameters just to show different runs in MLflow UI
    n_estimators = random.choice([50, 100, 150, 200])
    max_depth = random.choice([None, 5, 10, 20])
    train_and_log_model(n_estimators=n_estimators, max_depth=max_depth)

with DAG(
    '08_mlops_mlflow_pipeline',
    default_args=default_args,
    description='MLOps Pipeline integrating Airflow and MLflow for Breast Cancer Classification',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mlops', 'mlflow', 'portfolio'],
) as dag:

    # Task 1: Extract and prepare data
    task_prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data,
    )

    # Task 2: Train the model and log to MLflow
    task_train_model = PythonOperator(
        task_id='train_model',
        python_callable=run_training,
    )

    # Task 3: Evaluate the newly trained model on the test set
    task_evaluate_model = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )

    # Define the DAG dependencies
    task_prepare_data >> task_train_model >> task_evaluate_model
