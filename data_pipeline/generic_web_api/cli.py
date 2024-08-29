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
    LOGGER.info(f"Completed ETL for pipeline: {data_pipeline_id}")


def main():
    multi_web_api_config = get_multi_web_api_config()
    for data_pipeline_id, _web_api_config_dict in (
        multi_web_api_config.web_api_config_dict_by_pipeline_id.items()
    ):
        LOGGER.info(f"Starting ETL for pipeline: {data_pipeline_id}")
        web_api_data_etl(data_pipeline_id)
    LOGGER.info('All ETL processes completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
