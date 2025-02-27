import logging

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledQueryPipelineConfig
)


LOGGER = logging.getLogger(__name__)


def process_processed_scheduled_query(pipeline_config: ScheduledQueryPipelineConfig):
    LOGGER.info('pipeline_config: %r', pipeline_config)


def process_processed_scheduled_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
    for pipeline_config in multi_config.scheduled_queries:
        process_processed_scheduled_query(pipeline_config)
