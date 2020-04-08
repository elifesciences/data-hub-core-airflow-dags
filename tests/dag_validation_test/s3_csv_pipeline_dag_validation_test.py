import pytest

from dags.s3_csv_import_controller import (
    DAG_ID as CONTROLLER_DAG_ID,
    TARGET_DAG as TARGET_DAG_ID
)
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def test_controller_dag_should_contain_one_task(dagbag):
    controller_dag = dagbag.get_dag(CONTROLLER_DAG_ID)
    assert len(controller_dag.tasks) == 1


def test_target_dag_should_contain_four_tasks(dagbag):
    target_dag = dagbag.get_dag(TARGET_DAG_ID)
    assert len(target_dag.tasks) == 4


@pytest.mark.parametrize(
    "dag_id, task_list",
    [
        (CONTROLLER_DAG_ID, ['trigger_s3_csv_etl_dag']),
        (TARGET_DAG_ID,
         ['Should_Remaining_Tasks_Execute',
          'Update_Previous_RunID_Variable_Value_For_DagRun_Locking',
          's3_key_sensor_task',
          'Etl_Csv'
          ])
    ],
)
def test_dag_should_contain_named_tasks(dagbag, dag_id, task_list):
    dag_should_contain_named_tasks(dagbag, dag_id, task_list)
