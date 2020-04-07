import pytest

from dags.web_api_import_controller import (
    DAG_ID as CONTROLLER_DAG,
    TARGET_DAG
)
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def test_target_dag_should_contain_one_task(dagbag):
    target_dag = dagbag.get_dag(TARGET_DAG)
    assert len(target_dag.tasks) == 1


def test_controller_dag_should_contain_one_task(dagbag):
    controller_dag = dagbag.get_dag(CONTROLLER_DAG)
    assert len(controller_dag.tasks) == 1


@pytest.mark.parametrize(
    "dag_id, task_list",
    [
        (CONTROLLER_DAG, ['trigger_web_api_etl_dag']),
        (TARGET_DAG,
         ['web_api_data_etl'])
    ],
)
def test_dag_should_contain_named_tasks(dagbag, dag_id, task_list):
    dag_should_contain_named_tasks(dagbag, dag_id, task_list)
