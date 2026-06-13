from datetime import datetime
import os
from airflow import DAG, Dataset
from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode


# GCP Configuration
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'your-gcp-project-id')
BQ_DATASET = 'ecommerce_raw'

# Define the Dataset that triggers this DAG
ecommerce_raw_dataset = Dataset(f"bigquery://{GCP_PROJECT_ID}/{BQ_DATASET}/all_tables")

# Cosmos Configuration
DBT_PROJECT_PATH = "/opt/airflow/dbt_project"
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

profile_config = ProfileConfig(
    profile_name="ecommerce",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    execution_mode=ExecutionMode.LOCAL,
)

with DAG(
    dag_id="06_dbt_warehouse_dag",
    schedule=[ecommerce_raw_dataset],  # Data-Aware Scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "portfolio"],
    description="Advanced dbt Orchestration using Astronomer Cosmos",
) as dag:
    
    # This TaskGroup will automatically parse the dbt project and create a task for each model
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )
