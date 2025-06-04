from datetime import date
import logging
import time

from data_pipeline.utils.data_store.bq_data_service import (
    get_bq_client
)

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledQueryPipelineConfig
)


LOGGER = logging.getLogger(__name__)


def replace_start_date_in_sql_query(
    sql_query: str,
    start_date: date = date.fromisoformat('2025-05-25')
) -> str:
    return sql_query.replace('{start_date}', start_date.isoformat().replace('-', ''))


def process_scheduled_query(pipeline_config: ScheduledQueryPipelineConfig):
    LOGGER.info('pipeline_config: %r', pipeline_config)
    updated_sql_query = replace_start_date_in_sql_query(
        pipeline_config.bigquery.sql_query
    )
    LOGGER.info('Running SQL Query: %r', updated_sql_query)

    start_time = time.perf_counter()

    client = get_bq_client(project=pipeline_config.bigquery.project_name)
    query_job = client.query(updated_sql_query)
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


def process_scheduled_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
    for pipeline_config in multi_config.scheduled_queries:
        process_scheduled_query(pipeline_config)
