import logging

from data_pipeline.scheduled_queries.pipeline_config import (
    get_multi_scheduled_queries_pipeline_config
)

from data_pipeline.scheduled_queries.pipeline import (
    process_processed_scheduled_queries
)

LOGGER = logging.getLogger(__name__)


def main():
    LOGGER.info('Starting Scheduled Queries pipeline')
    multi_config = get_multi_scheduled_queries_pipeline_config()
    process_processed_scheduled_queries(multi_config)
    LOGGER.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
