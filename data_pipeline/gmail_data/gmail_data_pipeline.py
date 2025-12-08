import logging

from data_pipeline.gmail_data.get_gmail_data_config import (
    MultiGmailDataConfig
)
from data_pipeline.utils.pipeline_config import get_pipeline_config_for_env_name_and_config_parser


LOGGER = logging.getLogger(__name__)


class GmailPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'GMAIL_DATA_CONFIG_FILE_PATH'


def get_multi_gmail_data_config() -> MultiGmailDataConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        GmailPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        MultiGmailDataConfig.from_dict
    )
