import time
import logging
from typing import NamedTuple, Optional
from tests.end2end_test.end_to_end_test_helper import (
    simple_query
)
from data_pipeline.utils.data_store.s3_data_service import delete_s3_object

LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods, missing-class-docstring
class DataPipelineCloudResource(NamedTuple):
    project_name: str
    dataset_name: str
    table_name: str
    state_file_bucket_name: Optional[str] = None
    state_file_object_name: Optional[str] = None


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


def enable_and_trigger_dag_and_wait_for_success(
    airflow_api,
    dag_id: str,
    target_dag=None,
    dag_trigger_conf: dict = None
):
    airflow_api.unpause_dag(target_dag)
    execution_date = airflow_api.trigger_dag(
        dag_id=dag_id, conf=dag_trigger_conf
    )
    LOGGER.info("target_dag is %s", target_dag)
    wait_till_all_dag_run_ends(
        airflow_api, execution_date, dag_id, target_dag,
    )
    assert airflow_api.get_dag_status(dag_id, execution_date) == "success"


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
    if (
        pipeline_cloud_resource.state_file_bucket_name
        and pipeline_cloud_resource.state_file_object_name
    ):
        delete_statefile_if_exist(
            pipeline_cloud_resource.state_file_bucket_name,
            pipeline_cloud_resource.state_file_object_name
        )
    enable_and_trigger_dag_and_wait_for_success(
        airflow_api=airflow_api,
        dag_id=dag_id,
        target_dag=target_dag,
        dag_trigger_conf=dag_trigger_conf
    )
    loaded_table_row_count = get_table_row_count(
        pipeline_cloud_resource.project_name,
        pipeline_cloud_resource.dataset_name,
        pipeline_cloud_resource.table_name,
    )
    assert loaded_table_row_count > 0


def wait_untill_dag_run_is_successful(
    airflow_api,
    dag_id: str,
    execution_date: str
) -> None:
    dag_status: str
    while True:
        dag_status = airflow_api.get_dag_status(dag_id, execution_date)
        if dag_status not in {"running", "queued"}:
            break
        time.sleep(5)
        LOGGER.info("etl in progress (status: %r)", dag_status)
    assert dag_status == "success"


def wait_till_all_dag_run_ends(
        airflow_api, execution_date,
        dag_id, triggered_dag_id=None
):
    wait_untill_dag_run_is_successful(
        airflow_api=airflow_api,
        dag_id=dag_id,
        execution_date=execution_date
    )
    LOGGER.info("triggered_dag_id is %s", triggered_dag_id)
    if triggered_dag_id:
        while airflow_api.does_triggered_dag_run_end_with_success(triggered_dag_id):
            LOGGER.info(
                "waiting for the DAG triggered by the controller. Dag id is %s",
                triggered_dag_id
            )
            time.sleep(5)


class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
