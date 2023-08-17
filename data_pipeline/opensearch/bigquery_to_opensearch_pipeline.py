import logging
from typing import Sequence

from data_pipeline.opensearch.bigquery_to_opensearch_config import BigQueryToOpenSearchConfig


LOGGER = logging.getLogger(__name__)


def fetch_documents_from_bigquery_and_update_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    LOGGER.debug('processing config_list: %r', config_list)
