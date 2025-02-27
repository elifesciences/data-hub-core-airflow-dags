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


def process_scheduled_query(pipeline_config: ScheduledQueryPipelineConfig):
    LOGGER.info('pipeline_config: %r', pipeline_config)
    LOGGER.info('Running SQL Query: %r', pipeline_config.bigquery.sql_query)

    start_time = time.perf_counter()

    client = get_bq_client(project=pipeline_config.bigquery.project_name)
    query_job = client.query(pipeline_config.bigquery.sql_query)
    query_job.result()

    duration = time.perf_counter() - start_time

    total_bytes_billed = query_job.total_bytes_billed
    LOGGER.info(
        'Scheduled Query: %s bytes billed, took: %.3fs',
        total_bytes_billed,
        duration
    )


def process_scheduled_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
    for pipeline_config in multi_config.scheduled_queries:
        process_scheduled_query(pipeline_config)
