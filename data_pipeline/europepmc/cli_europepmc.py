import logging
from typing import Sequence

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig
from data_pipeline.europepmc.europepmc_pipeline import (
    fetch_article_data_from_europepmc_and_load_into_bigquery_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class EuropePmcPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'EUROPEPMC_CONFIG_FILE_PATH'


def get_europepmc_pipeline_config_list() -> Sequence[EuropePmcConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        EuropePmcPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        EuropePmcConfig.parse_config_list_from_dict
    )


def main():
    configs = get_europepmc_pipeline_config_list()
    fetch_article_data_from_europepmc_and_load_into_bigquery_from_config_list(configs)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
