import logging
from typing import Sequence

from data_pipeline.opensearch.bigquery_to_opensearch_config import BigQueryToOpenSearchConfig


LOGGER = logging.getLogger(__name__)


def fetch_documents_from_bigquery_and_update_opensearch(
    config: BigQueryToOpenSearchConfig
):
    LOGGER.debug('processing config: %r', config)


def fetch_documents_from_bigquery_and_update_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    for config in config_list:
        fetch_documents_from_bigquery_and_update_opensearch(config)
