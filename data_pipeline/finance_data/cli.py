import logging
from typing import Sequence
from data_pipeline.finance_data.finance_data_pipeline_config import (
    FinanceDataPipelineConfig
)
from data_pipeline.finance_data.finance_data_pipeline import (
    fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class FinanceDataEnvironmentVariables:
    CONFIG_FILE_PATH = 'FINANCE_DATA_CONFIG_FILE_PATH'


def get_finance_data_config_list() -> Sequence[FinanceDataPipelineConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        FinanceDataEnvironmentVariables.CONFIG_FILE_PATH,
        FinanceDataPipelineConfig.parse_config_list_from_dict
    )


def main():
    configs = get_finance_data_config_list()
    fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list(configs)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
