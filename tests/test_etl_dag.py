import os
import pytest
from airflow.models import DagBag

DAG_ID = os.path.basename(__file__).replace("test_", "").replace(".py", "")


@pytest.fixture(scope="session")
def dagbag():
    return DagBag()


@pytest.fixture(scope="session")
def dag(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    return dag


def test_dag_loaded(dagbag, dag):
    expect_import_errors = 0
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dagbag.import_errors) == expect_import_errors


def test_dag_structure(dag):
    expect_tasks_count = 2
    expect_tasks = {'etl_task', 'load_task'}
    assert len(dag.tasks) == expect_tasks_count
    assert dag.task_dict.keys() == expect_tasks


def test_task_dependencies(dag):
    tasks = dag.tasks
    dependencies = {
        'etl_task': {'downstream': [], 'upstream': []},
    }
    for task in tasks:
        assert task.downstream_task_ids == set(dependencies[task.task_id]['downstream'])
        assert task.upstream_task_ids == set(dependencies[task.task_id]['upstream'])
