import pytest

from dags.google_spreadsheet_pipeline_controller import (
    DAG_ID as CONTROLLER_DAG_ID,
    TARGET_DAG_ID
)


def test_dags_should_contain_one_task(dagbag):
    controller_dag = dagbag.get_dag(CONTROLLER_DAG_ID)
    target_dag = dagbag.get_dag(TARGET_DAG_ID)
    assert len(controller_dag.tasks) == 1
    assert len(target_dag.tasks) == 1


@pytest.mark.parametrize(
    "dag_id, task_list",
    [
        (CONTROLLER_DAG_ID, ['trigger_google_spreadsheet_etl_dag']),
        (TARGET_DAG_ID, ['google_spreadsheet_data_etl'])
    ],
)
def test_dag_should_contain_named_tasks(dagbag, dag_id, task_list):
    dag = dagbag.get_dag(dag_id)
    tasks = dag.tasks
    task_ids = list(map(lambda task: task.task_id, tasks))
    task_ids.sort()
    expected_ids = task_list
    expected_ids.sort()

    assert task_ids == expected_ids
