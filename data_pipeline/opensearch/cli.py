import logging
from typing import Sequence
from data_pipeline.opensearch.bigquery_to_opensearch_config import BigQueryToOpenSearchConfig
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class BigQueryToOpenSearchPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'BIGQUERY_TO_OPENSEARCH_CONFIG_FILE_PATH'


def get_bq_to_opensearch_pipeline_config_list() -> Sequence[BigQueryToOpenSearchConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        BigQueryToOpenSearchPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        BigQueryToOpenSearchConfig.parse_config_list_from_dict
    )


def main():
    configs = get_bq_to_opensearch_pipeline_config_list()
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(configs)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()