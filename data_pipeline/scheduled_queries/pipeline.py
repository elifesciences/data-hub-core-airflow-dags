import logging

from data_pipeline.utils.data_store.bq_data_service import (
    get_bq_result_from_bq_query
)

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledQueryPipelineConfig
)


LOGGER = logging.getLogger(__name__)


def process_scheduled_query(pipeline_config: ScheduledQueryPipelineConfig):
    LOGGER.info('pipeline_config: %r', pipeline_config)
    LOGGER.info('Running SQL Query: %r', pipeline_config.bigquery.sql_query)
    bq_result = get_bq_result_from_bq_query(
        project_name=pipeline_config.bigquery.project_name,
        query=pipeline_config.bigquery.sql_query
    )
    LOGGER.info('bq_result: %r', bq_result)


def process_scheduled_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
    for pipeline_config in multi_config.scheduled_queries:
        process_scheduled_query(pipeline_config)
