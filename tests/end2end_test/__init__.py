import time
import logging
from tests.end2end_test.end_to_end_test_helper import (
    simple_query
)
from data_pipeline.utils.data_store.s3_data_service import delete_s3_object

LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods, missing-class-docstring
class DataPipelineCloudResource:
    # pylint: disable=too-many-arguments
    def __init__(
            self, project_name, dataset_name, table_name,
            state_file_bucket_name, state_file_object_name
    ):
        self.project_name = project_name
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.state_file_bucket_name = state_file_bucket_name
        self.state_file_object_name = state_file_object_name


# pylint: disable=broad-except
def truncate_table(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=project_name,
            dataset=dataset_name,
            table=table_name,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")


def delete_statefile_if_exist(
        state_file_bucket_name,
        state_file_object_name
):

    try:
        delete_s3_object(state_file_bucket_name,
                         state_file_object_name
                         )
    except Exception:
        LOGGER.info("s3 object not deleted, may not exist")


def get_table_row_count(
        project_name,
        dataset_name,
        table_name
):
    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=project_name,
        dataset=dataset_name,
        table=table_name,
    )
    return query_response[0].get("count")


# pylint: disable=too-many-arguments
def trigger_run_test_pipeline(
        airflow_api,
        pipeline_cloud_resource: DataPipelineCloudResource,
        dag_id, target_dag=None,
        dag_trigger_conf: dict = None
):

    truncate_table(
        pipeline_cloud_resource.project_name,
        pipeline_cloud_resource.dataset_name,
        pipeline_cloud_resource.table_name,
    )
    delete_statefile_if_exist(
        pipeline_cloud_resource.state_file_bucket_name,
        pipeline_cloud_resource.state_file_object_name
    )
    airflow_api.unpause_dag(target_dag)
    execution_date = airflow_api.trigger_dag(
        dag_id=dag_id, conf=dag_trigger_conf
    )
    is_dag_running = wait_till_all_dag_run_ends(
        airflow_api, execution_date, dag_id, target_dag,
    )
    assert not is_dag_running
    assert airflow_api.get_dag_status(dag_id, execution_date) == "success"
    loaded_table_row_count = get_table_row_count(
        pipeline_cloud_resource.project_name,
        pipeline_cloud_resource.dataset_name,
        pipeline_cloud_resource.table_name,
    )
    assert loaded_table_row_count > 0


def wait_till_all_dag_run_ends(
        airflow_api, execution_date,
        dag_id, triggered_dag_id=None
):
    is_dag_running = True
    while is_dag_running:
        is_dag_running = airflow_api.is_dag_running(dag_id, execution_date)
        if not is_dag_running and triggered_dag_id:
            time.sleep(15)
            is_dag_running = airflow_api.is_triggered_dag_running(
                triggered_dag_id
            )
        LOGGER.info("etl in progress")
        time.sleep(5)
    time.sleep(15)
    return is_dag_running


class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
