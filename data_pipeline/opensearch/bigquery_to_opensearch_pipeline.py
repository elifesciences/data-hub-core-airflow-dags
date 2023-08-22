import logging
from typing import Iterable, Sequence

from opensearchpy import OpenSearch
import opensearchpy

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchFieldNamesForConfig,
    OpenSearchTargetConfig
)
from data_pipeline.utils.collections import iter_batch_iterable
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.utils.pipeline_utils import iter_dict_from_bq_query_for_bigquery_source_config


LOGGER = logging.getLogger(__name__)


def iter_documents_from_bigquery(
    bigquery_source_config: BigQuerySourceConfig
) -> Iterable[dict]:
    LOGGER.debug('processing bigquery source config: %r', bigquery_source_config)
    return (
        remove_key_with_null_value(document)
        for document in iter_dict_from_bq_query_for_bigquery_source_config(
            bigquery_source_config
        )
    )


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
    index_name = opensearch_target_config.index_name
    index_settings = opensearch_target_config.index_settings
    LOGGER.info('index_name: %r', index_name)
    LOGGER.info('index_settings: %r', index_settings)
    index_exists = client.indices.exists(index_name)
    LOGGER.info('index_exists: %r', index_exists)
    if index_exists:
        if index_settings:
            if opensearch_target_config.update_index_settings:
                LOGGER.info('updating index settings: %r', index_settings)
                client.indices.put_settings(index=index_name, body=index_settings)
            mappings = index_settings.get('mappings')
            if mappings and opensearch_target_config.update_mappings:
                LOGGER.info('updating mappings: %r', mappings)
                client.indices.put_mapping(index=index_name, body=mappings)
    else:
        client.indices.create(index=index_name, body=index_settings)


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
    opensearch_target_config: OpenSearchTargetConfig,
    field_names_for_config: BigQueryToOpenSearchFieldNamesForConfig,
    batch_size: int
):
    LOGGER.debug('loading documents into opensearch: %r', document_iterable)
    bulk_action_iterable = iter_opensearch_bulk_action_for_documents(
        document_iterable,
        index_name=opensearch_target_config.index_name,
        id_field_name=field_names_for_config.id
    )
    streaming_bulk_result_iterable = opensearchpy.helpers.streaming_bulk(
        client=client,
        actions=bulk_action_iterable,
        chunk_size=batch_size
    )
    for _ in streaming_bulk_result_iterable:
        pass


def create_or_update_index_and_load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    opensearch_target_config: OpenSearchTargetConfig,
    field_names_for_config: BigQueryToOpenSearchFieldNamesForConfig,
    batch_size: int
):
    client = get_opensearch_client(opensearch_target_config)
    LOGGER.info('client: %r', client)
    create_or_update_opensearch_index(
        client=client,
        opensearch_target_config=opensearch_target_config
    )
    for batch_documents_iterable in iter_batch_iterable(document_iterable, batch_size):
        batch_documents = list(batch_documents_iterable)
        load_documents_into_opensearch(
            batch_documents,
            client=client,
            opensearch_target_config=opensearch_target_config,
            field_names_for_config=field_names_for_config,
            batch_size=len(batch_documents)
        )


def fetch_documents_from_bigquery_and_load_into_opensearch(
    config: BigQueryToOpenSearchConfig
):
    LOGGER.debug('processing config: %r', config)
    document_iterable = iter_documents_from_bigquery(config.source.bigquery)
    create_or_update_index_and_load_documents_into_opensearch(
        document_iterable,
        opensearch_target_config=config.target.opensearch,
        field_names_for_config=config.field_names_for,
        batch_size=config.batch_size
    )


def fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    for config in config_list:
        fetch_documents_from_bigquery_and_load_into_opensearch(config)
