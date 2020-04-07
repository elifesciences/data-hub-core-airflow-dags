def dag_should_contain_named_tasks(dagbag, dag_id, task_list):
    dag = dagbag.get_dag(dag_id)
    tasks = dag.tasks
    task_ids = list(map(lambda task: task.task_id, tasks))
    task_ids.sort()
    expected_ids = task_list
    expected_ids.sort()

    assert task_ids == expected_ids
