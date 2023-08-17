import logging
from typing import Iterable, Sequence

from data_pipeline.opensearch.bigquery_to_opensearch_config import BigQueryToOpenSearchConfig
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig


LOGGER = logging.getLogger(__name__)


def iter_documents_from_bigquery(
    bigquery_source_config: BigQuerySourceConfig
) -> Iterable[dict]:
    LOGGER.debug('processing bigquery source config: %r', bigquery_source_config)
    return []


def load_documents_into_opensearch(
    document_iterable: Iterable[dict]
):
    LOGGER.debug('loading documents into opensearch: %r', document_iterable)


def fetch_documents_from_bigquery_and_load_into_opensearch(
    config: BigQueryToOpenSearchConfig
):
    LOGGER.debug('processing config: %r', config)
    document_iterable = iter_documents_from_bigquery(config.source.bigquery)
    load_documents_into_opensearch(document_iterable)


def fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    for config in config_list:
        fetch_documents_from_bigquery_and_load_into_opensearch(config)
