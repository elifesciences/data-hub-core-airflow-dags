import logging

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig
)


LOGGER = logging.getLogger(__name__)


def process_processed_queries(multi_config: MultiScheduledQueryPipelineConfig):
    LOGGER.info('multi_config: %r', multi_config)
