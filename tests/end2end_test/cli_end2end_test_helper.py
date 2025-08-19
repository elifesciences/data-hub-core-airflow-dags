import logging
from typing import List, NamedTuple, Optional
from google.cloud import bigquery

from data_pipeline.utils.data_store.s3_data_service import delete_s3_object

LOGGER = logging.getLogger(__name__)


class TestQueryTemplate:
    CLEAN_TABLE_QUERY = '''
    DELETE FROM `{project}.{dataset}.{table}` WHERE True
    '''
    READ_COUNT_TABLE_QUERY = '''
    SELECT COUNT(*) AS count FROM `{project}.{dataset}.{table}`
    '''


class DataPipelineCloudResource(NamedTuple):
    project_name: str
    dataset_name: str
    table_name: str
    state_file_bucket_name: Optional[str] = None
    state_file_object_name: Optional[str] = None


def simple_query(project: str, dataset: str, table: str, query: str) -> List[dict]:
    bigquery_client = bigquery.Client(project=project)
    _query = query.format(project=project, dataset=dataset, table=table).strip()
    LOGGER.info('running query:\n%s', _query)
    query_job = bigquery_client.query(_query)
    rows = [dict(row) for row in query_job]
    LOGGER.debug('rows: %s', rows)
    return rows


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
    except Exception:  # pylint: disable=broad-except
        LOGGER.info('table not cleaned, maybe it does not exist')


def delete_statefile_if_exist(
    state_file_bucket_name,
    state_file_object_name
):
    try:
        delete_s3_object(state_file_bucket_name, state_file_object_name)
    except Exception:  # pylint: disable=broad-except
        LOGGER.info('s3 object not deleted, may not exist')


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
    return query_response[0].get('count')


def clean_before_test(
    pipeline_cloud_resource: DataPipelineCloudResource
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


def check_after_test(
    pipeline_cloud_resource: DataPipelineCloudResource
):
    loaded_table_row_count = get_table_row_count(
        pipeline_cloud_resource.project_name,
        pipeline_cloud_resource.dataset_name,
        pipeline_cloud_resource.table_name,
    )
    assert loaded_table_row_count > 0
