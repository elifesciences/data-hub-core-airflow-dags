import argparse
import logging
from data_pipeline.gmail_data.get_gmail_data_config import GmailDataConfig
from data_pipeline.gmail_data.gmail_data_pipeline import get_multi_gmail_data_config
from data_pipeline.utils.pipeline_config import get_deployment_env

LOGGER = logging.getLogger(__name__)


def gmail_data_etl(data_pipeline_id: str):
    multi_config = get_multi_gmail_data_config()
    LOGGER.debug('multi_config: %s', multi_config)
    data_config_dict = multi_config.gmail_data_config[data_pipeline_id]
    data_config = GmailDataConfig.from_dict(
        data_config=data_config_dict,
        deployment_env=get_deployment_env()
    )
    LOGGER.info('Gmail Data Config: %s', data_config)


def main():
    parser = argparse.ArgumentParser(description="Run ETL for a specific Web API pipeline")
    parser.add_argument('--data-pipeline-id', required=True)
    args = parser.parse_args()
    data_pipeline_id = args.data_pipeline_id
    LOGGER.info('Starting ETL for pipeline: %s', data_pipeline_id)
    gmail_data_etl(data_pipeline_id)
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
