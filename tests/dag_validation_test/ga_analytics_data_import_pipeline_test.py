from dags.google_analytics_data_import_pipeline import (
    DAG_ID
)
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def test_dag_should_contain_n_task(dagbag):
    target_dag = dagbag.get_dag(DAG_ID)
    assert len(target_dag.tasks) == 1


def test_dag_should_contain_named_tasks(dagbag):
    ga_task_list = [
        'etl_google_analytics'
    ]
    dag_should_contain_named_tasks(dagbag, DAG_ID, ga_task_list)
