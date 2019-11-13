"""
test for  dags
"""
import importlib
import os
import pytest
from airflow import models as af_models


DAG_PATH = os.path.join(os.path.dirname(__file__), "../..", "dags")
DAG_FILES = [f for f in os.listdir(DAG_PATH) if f.endswith("pipeline.py")]


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    """Import dag files and check for DAG."""
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)

    mod_spec = importlib.util.spec_from_file_location(
        module_name, module_path)

    module = importlib.util.module_from_spec(mod_spec)

    mod_spec.loader.exec_module(module)

    dag_objects = [
        var for var in vars(module).values() if isinstance(
            var, af_models.DAG)]
    assert len(dag_objects) > 0

    for dag in dag_objects:
        dag.test_cycle()


def test_import_dags():
    """
    :return:
    """
    dagbag_data_pipeline = af_models.DagBag(dag_folder=DAG_PATH)
    assert len(dagbag_data_pipeline.import_errors) == 0, \
        "DAG import failures. Errors: {}".format(
            dagbag_data_pipeline.import_errors)


def test_task_count():
    """
    :return:
    """
    dagbag = af_models.DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag_id = 'Load_Crossref_Event_Into_Bigquery'
    dag = dagbag.get_dag(dag_id)
    assert len(dag.tasks) == 6


def test_contain_tasks():
    """
    :return:
    """
    dagbag = af_models.DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag_id = 'Load_Crossref_Event_Into_Bigquery'
    dag = dagbag.get_dag(dag_id)

    tasks = dag.tasks
    task_ids = list(map(lambda task: task.task_id, tasks))
    task_ids = task_ids.sort()
    expected_ids = [
        'get_data_config',
        'create_table_if_not_exist',
        'download_and_semi_transform_crossref_data',
        'load_data_to_bigquery',
        'cleanup_file',
        'log_last_execution_and_cleanup']
    expected_ids = expected_ids.sort()

    assert task_ids == expected_ids


def test_dependencies_of_load_data_to_bigquery_task():
    """
    :return:
    """
    dagbag = af_models.DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag_id = 'Load_Crossref_Event_Into_Bigquery'
    dag = dagbag.get_dag(dag_id)
    load_data_to_bigquery_task = dag.get_task('load_data_to_bigquery')

    upstream_task_ids = list(
        map(lambda task: task.task_id,
            load_data_to_bigquery_task.upstream_list))
    assert upstream_task_ids == [
        'download_and_semi_transform_crossref_data']
    downstream_task_ids = list(
        map(lambda task: task.task_id,
            load_data_to_bigquery_task.downstream_list))
    downstream_task_ids = downstream_task_ids.sort()
    expected_downstream_task_ids = [
        'log_last_execution_and_cleanup', 'cleanup_file']
    expected_downstream_task_ids = expected_downstream_task_ids.sort()
    assert downstream_task_ids == expected_downstream_task_ids
