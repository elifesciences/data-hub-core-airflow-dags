import logging
from typing import Iterable, Sequence

from opensearchpy import OpenSearch

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    OpenSearchTargetConfig
)
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig


LOGGER = logging.getLogger(__name__)


def iter_documents_from_bigquery(
    bigquery_source_config: BigQuerySourceConfig
) -> Iterable[dict]:
    LOGGER.debug('processing bigquery source config: %r', bigquery_source_config)
    return []


def get_opensearch_client(opensearch_target_config: OpenSearchTargetConfig) -> OpenSearch:
    LOGGER.debug('opensearch_target_config: %r', opensearch_target_config)
    return OpenSearch(
        hosts=[{
            'host': opensearch_target_config.hostname,
            'port': opensearch_target_config.port
        }],
        http_auth=(opensearch_target_config.username, opensearch_target_config.password),
        use_ssl=True,
        verify_certs=opensearch_target_config.verify_certificates
    )


def load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    client: OpenSearch,
    opensearch_target_config: OpenSearchTargetConfig
):
    LOGGER.debug('loading documents into opensearch: %r', document_iterable)
    LOGGER.info('index_name: %r', opensearch_target_config.index_name)
    index_exists = client.indices.exists(opensearch_target_config.index_name)
    LOGGER.info('index_exists: %r', index_exists)


def create_or_update_index_and_load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    opensearch_target_config: OpenSearchTargetConfig
):
    client = get_opensearch_client(opensearch_target_config)
    LOGGER.info('client: %r', client)
    load_documents_into_opensearch(
        document_iterable,
        client=client,
        opensearch_target_config=opensearch_target_config
    )


def fetch_documents_from_bigquery_and_load_into_opensearch(
    config: BigQueryToOpenSearchConfig
):
    LOGGER.debug('processing config: %r', config)
    document_iterable = iter_documents_from_bigquery(config.source.bigquery)
    create_or_update_index_and_load_documents_into_opensearch(
        document_iterable,
        opensearch_target_config=config.target.opensearch
    )


def fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    for config in config_list:
        fetch_documents_from_bigquery_and_load_into_opensearch(config)
