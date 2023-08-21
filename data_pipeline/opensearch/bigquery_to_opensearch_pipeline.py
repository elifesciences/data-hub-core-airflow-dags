import logging
from typing import Iterable, Sequence

from opensearchpy import OpenSearch
import opensearchpy

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


def create_or_update_opensearch_index(
    client: OpenSearch,
    opensearch_target_config: OpenSearchTargetConfig
):
    LOGGER.info('index_name: %r', opensearch_target_config.index_name)
    LOGGER.info('index_settings: %r', opensearch_target_config.index_settings)
    index_exists = client.indices.exists(opensearch_target_config.index_name)
    LOGGER.info('index_exists: %r', index_exists)
    if index_exists:
        if opensearch_target_config.index_settings:
            client.indices.put_settings(
                index=opensearch_target_config.index_name,
                body=opensearch_target_config.index_settings
            )
            mappings = opensearch_target_config.index_settings.get('mappings')
            if mappings:
                client.indices.put_mapping(
                    index=opensearch_target_config.index_name,
                    body=mappings
                )
    else:
        client.indices.create(
            index=opensearch_target_config.index_name,
            body=opensearch_target_config.index_settings
        )


def get_opensearch_bulk_action_for_document(
    document: dict,
    index_name: str,
    id_field_name: str
) -> dict:
    return {
        '_op_type': 'index',
        '_index': index_name,
        '_id': document[id_field_name],
        '_source': document
    }


def iter_opensearch_bulk_action_for_documents(
    document_iterable: Iterable[dict],
    index_name: str,
    id_field_name: str
) -> Iterable[dict]:
    return (
        get_opensearch_bulk_action_for_document(
            document,
            index_name=index_name,
            id_field_name=id_field_name
        )
        for document in document_iterable
    )


def load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    client: OpenSearch,
    opensearch_target_config: OpenSearchTargetConfig
):
    LOGGER.debug('loading documents into opensearch: %r', document_iterable)
    bulk_action_iterable = iter_opensearch_bulk_action_for_documents(
        document_iterable,
        index_name=opensearch_target_config.index_name,
        id_field_name='doi'
    )
    streaming_bulk_result_iterable = opensearchpy.helpers.streaming_bulk(
        client=client,
        actions=bulk_action_iterable
    )
    for _ in streaming_bulk_result_iterable:
        pass


def create_or_update_index_and_load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    opensearch_target_config: OpenSearchTargetConfig
):
    client = get_opensearch_client(opensearch_target_config)
    LOGGER.info('client: %r', client)
    create_or_update_opensearch_index(
        client=client,
        opensearch_target_config=opensearch_target_config
    )
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
