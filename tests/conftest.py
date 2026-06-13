import pytest
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook

@pytest.fixture
def mock_dbt_cloud_hook():
    return DbtCloudHook(
        account_id=123,
        job_id=456,
        api_token='test-token'
    )

@pytest.fixture
def mock_airflow_context():
    return {
        "ds": "2023-01-01",
        "ts": "2023-01-01T00:00:00",
        "dag_run": "test_dag_run",
        "dag": "test_dag",
        "task": "test_task",
        "logical_date": "2023-01-01",
        "execution_date": "2023-01-01",
        "next_execution_date": "2023-01-02",
        "prev_execution_date": "2022-12-31",
        "prev_execution_date_success": "2022-12-31",
        "ti": "test_ti",
        "run_id": "test_run_id",
        "dag_run_id": "test_dag_run_id",
        "task_instance_key_str": "test_task_instance_key_str",
    }

@pytest.fixture
def mock_dbt_test_result():
    return {
        "name": "test_model.test_not_null",
        "status": "pass",
        "message": "OK",
        "execution_time": 0.1,
    }

@pytest.fixture
def mock_dbt_test_results():
    return [
        {
            "name": "test_model.test_not_null",
            "status": "pass",
            "message": "OK",
            "execution_time": 0.1,
        },
        {
            "name": "test_model.test_unique",
            "status": "fail",
            "message": "FAIL",
            "execution_time": 0.1,
        },
    ]

@pytest.fixture
def mock_dbt_test_results_filtered():
    return [
        {
            "name": "test_model.test_not_null",
            "status": "pass",
            "message": "OK",
            "execution_time": 0.1,
        },
        {
            "name": "test_model.test_unique",
            "status": "pass",
            "message": "OK",
            "execution_time": 0.1,
        },
    ]

@pytest.fixture
def mock_dbt_test_results_filtered_by_status():
    return [
        {
            "name": "test_model.test_not_null",
            "status": "pass",
            "message": "OK",
            "execution_time": 0.1,
        },
        {
            "name": "test_model.test_unique",
            "status": "pass",
            "message": "OK",
            "execution_time": 0.1,
        },
    ]

