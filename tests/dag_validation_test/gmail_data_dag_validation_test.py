from dags.gmail_data_import_pipeline import (
    DAG_ID
)

from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def test_dag_should_contain_n_task(dagbag):
    target_dag = dagbag.get_dag(DAG_ID)
    assert len(target_dag.tasks) == 11


def test_dag_should_contain_named_tasks(dagbag):
    gm_task_list = [
        'gmail_label_data_to_temp_table_etl',
        'gmail_thread_ids_list_to_temp_table_etl',
        'gmail_history_details_to_temp_table_etl',
        'gmail_thread_details_from_temp_thread_ids_etl',
        'gmail_thread_details_from_temp_history_details_etl',
        'delete_temp_table_labels',
        'delete_temp_table_thread_ids',
        'delete_temp_table_history_details',
        'load_from_temp_table_to_label_list',
        'load_from_temp_table_to_thread_ids_list'
    ]
    dag_should_contain_named_tasks(dagbag, DAG_ID, gm_task_list)
