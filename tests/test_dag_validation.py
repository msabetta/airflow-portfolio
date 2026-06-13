import os
from airflow.models.dagbag import DagBag
from conftest import mock_airflow_context, mock_dbt_test_results

def test_dag_validation():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert len(dag_bag.dags) > 0, "No DAGs found"
    
def test_dag_syntax():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.is_paused is False for dag in dag_bag.dags), "Some DAGs are paused"

def test_dag_tasks():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(len(dag.tasks) > 0 for dag in dag_bag.dags), "Some DAGs have no tasks"

def test_dag_start_date():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.start_date is not None for dag in dag_bag.dags), "Some DAGs have no start date"

def test_dag_schedule_interval():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.schedule_interval is not None for dag in dag_bag.dags), "Some DAGs have no schedule interval"

def test_dag_default_args():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.default_args is not None for dag in dag_bag.dags), "Some DAGs have no default args"

def test_dag_tags():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.tags is not None for dag in dag_bag.dags), "Some DAGs have no tags"

def test_dag_description():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.description is not None for dag in dag_bag.dags), "Some DAGs have no description"

def test_dag_catchup():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.catchup is not None for dag in dag_bag.dags), "Some DAGs have no catchup"

def test_dag_max_active_runs():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.max_active_runs is not None for dag in dag_bag.dags), "Some DAGs have no max active runs"

def test_dag_max_active_tasks():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.max_active_tasks is not None for dag in dag_bag.dags), "Some DAGs have no max active tasks"

def test_dag_default_view():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.default_view is not None for dag in dag_bag.dags), "Some DAGs have no default view"

def test_dag_user_defined_filters():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.user_defined_filters is not None for dag in dag_bag.dags), "Some DAGs have no user defined filters"

def test_dag_user_defined_macros():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.user_defined_macros is not None for dag in dag_bag.dags), "Some DAGs have no user defined macros"

def test_dag_user_defined_macros():
    dag_bag = DagBag()
    dag_bag.dag_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag.collect()
    assert all(dag.user_defined_macros is not None for dag in dag_bag.dags), "Some DAGs have no user defined macros"