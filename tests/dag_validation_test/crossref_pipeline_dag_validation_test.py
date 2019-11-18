"""
test for  dags
"""
DAG_ID = 'Load_Crossref_Event_Into_Bigquery'


def test_task_count(dagbag):
    """
    :return:
    """
    dag = dagbag.get_dag(DAG_ID)
    assert len(dag.tasks) == 4


def test_contain__all_tasks(dagbag):
    """
    :return:
    """
    dag = dagbag.get_dag(DAG_ID)

    tasks = dag.tasks
    task_ids = list(map(lambda task: task.task_id, tasks))
    task_ids.sort()
    expected_ids = ['create_table_if_not_exist',
                    'crossref_event_data_etl',
                    'get_data_config',
                    'log_last_record_date'
                    ]

    expected_ids.sort()

    assert task_ids == expected_ids


def test_dependencies_of_crossref_event_data_etl_task(dagbag):
    """
    :return:
    """
    dag = dagbag.get_dag(DAG_ID)
    crossref_data_etl_task = dag.get_task('crossref_event_data_etl')

    upstream_task_ids = list(
        map(lambda task: task.task_id,
            crossref_data_etl_task.upstream_list))
    assert upstream_task_ids == [
        'create_table_if_not_exist']
    downstream_task_ids = list(
        map(lambda task: task.task_id,
            crossref_data_etl_task.downstream_list))
    downstream_task_ids = downstream_task_ids.sort()
    expected_downstream_task_ids = ['log_last_record_date']
    expected_downstream_task_ids = expected_downstream_task_ids.sort()
    assert downstream_task_ids == expected_downstream_task_ids
