import logging
from typing import Sequence

from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig
)
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    fetch_article_dois_from_bigquery_and_update_labslink_ftp
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class EuropePmcLabsLinkPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'EUROPEPMC_LABSLINK_CONFIG_FILE_PATH'


def get_europepmc_labslink_pipeline_config_list() -> Sequence[EuropePmcLabsLinkConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        EuropePmcLabsLinkPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        EuropePmcLabsLinkConfig.from_dict
    )


def main():
    configs = get_europepmc_labslink_pipeline_config_list()
    fetch_article_dois_from_bigquery_and_update_labslink_ftp(configs)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
