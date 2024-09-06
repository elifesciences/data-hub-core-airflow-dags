import argparse
import logging
from data_pipeline.generic_web_api.generic_web_api_config import (
    MultiWebApiConfig,
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    generic_web_api_data_etl
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class WebApiEnvironmentVariables:
    CONFIG_FILE_PATH = 'WEB_API_CONFIG_FILE_PATH'


def get_multi_web_api_config() -> MultiWebApiConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        WebApiEnvironmentVariables.CONFIG_FILE_PATH,
        MultiWebApiConfig
    )


def web_api_data_etl(data_pipeline_id: str):
    multi_web_api_config = get_multi_web_api_config()
    data_config_dict = multi_web_api_config.web_api_config_dict_by_pipeline_id[data_pipeline_id]
    data_config = WebApiConfig.from_dict(data_config_dict)
    generic_web_api_data_etl(data_config=data_config)
    LOGGER.info('Completed ETL for pipeline: %s', data_pipeline_id)


def main():
    parser = argparse.ArgumentParser(description="Run ETL for a specific Web API pipeline")
    parser.add_argument('--data-pipeline-id', required=True)
    args = parser.parse_args()
    data_pipeline_id = args.data_pipeline_id
    LOGGER.info('Starting ETL for pipeline: %s', data_pipeline_id)
    web_api_data_etl(data_pipeline_id)
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
