from datetime import date
import logging
import time

from data_pipeline.utils.data_store.bq_data_service import (
    get_bq_client
)

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledQueryPipelineConfig,
    ScheduledQueryPipelineStateConfig
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    upload_s3_object
)
from data_pipeline.utils.pipeline_config import StateFileConfig


LOGGER = logging.getLogger(__name__)


def replace_start_date_in_sql_query(
    sql_query: str,
    start_date: date
) -> str:
    return sql_query.replace('{start_date}', start_date.isoformat().replace('-', ''))


def update_state_file(
    state_file: StateFileConfig,
    current_date: date
) -> None:
    upload_s3_object(
        bucket=state_file.bucket_name,
        object_key=state_file.object_name,
        data_object=current_date.isoformat()
    )
    LOGGER.info(
        'Updated state file: s3://%s/%s with current date: %s',
        state_file.bucket_name,
        state_file.object_name,
        current_date.isoformat()
    )


def load_state_or_default_from_s3_for_config(
    state_config: ScheduledQueryPipelineStateConfig
) -> date:
    try:
        return date.fromisoformat(
            download_s3_object_as_string_or_file_not_found_error(
                bucket=state_config.state_file.bucket_name,
                object_key=state_config.state_file.object_name
            )
        )
    except FileNotFoundError:
        LOGGER.info('state file not found, returning initial state')
        return state_config.initial_state.start_date


def process_scheduled_query(pipeline_config: ScheduledQueryPipelineConfig):
    LOGGER.info('pipeline_config: %r', pipeline_config)

    sql_query = pipeline_config.bigquery.sql_query
    if pipeline_config.state:
        sql_query = replace_start_date_in_sql_query(
            sql_query=pipeline_config.bigquery.sql_query,
            start_date=pipeline_config.state.initial_state.start_date
        )

    LOGGER.info('Running SQL Query: %r', sql_query)

    start_time = time.perf_counter()

    client = get_bq_client(project=pipeline_config.bigquery.project_name)
    query_job = client.query(sql_query)
    query_job.result()

    duration = time.perf_counter() - start_time
    total_bytes_billed = query_job.total_bytes_billed
    slot_millis = query_job.slot_millis

    LOGGER.info(
        (
            'Scheduled Query: data_pipeline_id=%s, statement_type=%s, destination=%s,'
            ' num_dml_affected_rows=%s, total_bytes_billed=%s, slot_time=%.3fs, took=%.3fs'
        ),
        pipeline_config.data_pipeline_id,
        query_job.statement_type,
        query_job.destination,
        query_job.num_dml_affected_rows,
        total_bytes_billed,
        slot_millis / 1000,
        duration
    )

    if pipeline_config.state:
        update_state_file(
            state_file=pipeline_config.state.state_file,
            current_date=date.today()
        )


def process_scheduled_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
    for pipeline_config in multi_config.scheduled_queries:
        process_scheduled_query(pipeline_config)
