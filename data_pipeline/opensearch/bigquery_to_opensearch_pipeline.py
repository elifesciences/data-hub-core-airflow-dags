import dataclasses
from datetime import datetime
import logging
from typing import Any, Iterable, Sequence

from opensearchpy import OpenSearch
import opensearchpy

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchFieldNamesForConfig,
    BigQueryToOpenSearchStateConfig,
    OpenSearchIngestionPipelineConfig,
    OpenSearchOperationModes,
    OpenSearchTargetConfig
)
from data_pipeline.utils.collections import iter_batch_iterable
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    upload_s3_object
)
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.pipeline_utils import iter_dict_from_bq_query_for_bigquery_source_config


LOGGER = logging.getLogger(__name__)


def get_wrapped_query(
    query: str,
    timestamp_field_name: str,
    start_timestamp: datetime
) -> str:
    return '\n'.join([
        'SELECT * FROM (',
        query,
        ')',
        f"WHERE {timestamp_field_name} >= TIMESTAMP('{start_timestamp.isoformat()}')",
        f'ORDER BY {timestamp_field_name}'
    ])


def iter_documents_from_bigquery(
    config: BigQueryToOpenSearchConfig,
    start_timestamp: datetime
) -> Iterable[dict]:
    bigquery_source_config = config.source.bigquery
    LOGGER.debug(
        'processing bigquery source config: %r (%r)',
        bigquery_source_config, start_timestamp
    )
    wrapped_query = get_wrapped_query(
        bigquery_source_config.sql_query,
        timestamp_field_name='.'.join(config.field_names_for.timestamp_key_path),
        start_timestamp=start_timestamp
    )
    LOGGER.info('wrapped_query: %r', wrapped_query)
    return (
        remove_key_with_null_value(document)
        for document in iter_dict_from_bq_query_for_bigquery_source_config(
            dataclasses.replace(bigquery_source_config, sql_query=wrapped_query)
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
        timeout=opensearch_target_config.timeout,
        use_ssl=True,
        verify_certs=opensearch_target_config.verify_certificates,
        ssl_show_warn=opensearch_target_config.verify_certificates
    )


def create_or_update_opensearch_ingest_pipeline(
    client: OpenSearch,
    ingest_pipeline_config: OpenSearchIngestionPipelineConfig
):
    LOGGER.info('Creating or updating ingesting pipeline: %r', ingest_pipeline_config.name)
    client.ingest.put_pipeline(
        id=ingest_pipeline_config.name,
        body=ingest_pipeline_config.definition
    )


def create_or_update_opensearch_ingest_pipelines(
    client: OpenSearch,
    ingest_pipeline_config_list: Sequence[OpenSearchIngestionPipelineConfig]
):
    LOGGER.debug('ingest_pipeline_config_list: %r', ingest_pipeline_config_list)
    for ingest_pipeline_config in ingest_pipeline_config_list:
        create_or_update_opensearch_ingest_pipeline(
            client=client,
            ingest_pipeline_config=ingest_pipeline_config
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


def prepare_opensearch(
    client: OpenSearch,
    opensearch_target_config: OpenSearchTargetConfig
):
    create_or_update_opensearch_ingest_pipelines(
        client=client,
        ingest_pipeline_config_list=opensearch_target_config.ingestion_pipelines
    )
    create_or_update_opensearch_index(
        client=client,
        opensearch_target_config=opensearch_target_config
    )


OPENSEARCH_BULK_DOCUMENT_FIELD_BY_OPERATION_MODE = {
    OpenSearchOperationModes.INDEX: '_source',
    OpenSearchOperationModes.CREATE: '_source',
    OpenSearchOperationModes.UPDATE: 'doc'
}


def get_required_value_for_key_path(parent: dict, key_path: Sequence[str]) -> Any:
    result: Any = parent
    for key in key_path:
        result = result.get(key)
    assert result is not None
    return result


def get_opensearch_bulk_action_for_document(
    document: dict,
    index_name: str,
    id_key_path: Sequence[str],
    operation_mode: str,
    upsert: bool
) -> dict:
    document_field_name = OPENSEARCH_BULK_DOCUMENT_FIELD_BY_OPERATION_MODE[operation_mode]
    result: dict = {
        '_op_type': operation_mode,
        '_index': index_name,
        '_id': get_required_value_for_key_path(document, id_key_path),
        document_field_name: document
    }
    if upsert:
        result['doc_as_upsert'] = True
    return result


def iter_opensearch_bulk_action_for_documents(
    document_iterable: Iterable[dict],
    index_name: str,
    id_key_path: Sequence[str],
    operation_mode: str,
    upsert: bool = False
) -> Iterable[dict]:
    return (
        get_opensearch_bulk_action_for_document(
            document,
            index_name=index_name,
            id_key_path=id_key_path,
            operation_mode=operation_mode,
            upsert=upsert
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
        id_key_path=field_names_for_config.id_key_path,
        operation_mode=opensearch_target_config.operation_mode,
        upsert=opensearch_target_config.upsert
    )
    streaming_bulk_result_iterable = opensearchpy.helpers.streaming_bulk(
        client=client,
        actions=bulk_action_iterable,
        chunk_size=batch_size
    )
    for _ in streaming_bulk_result_iterable:
        pass


def save_state_to_s3_for_config(
    state_config: BigQueryToOpenSearchStateConfig,
    start_timestamp: datetime
):
    upload_s3_object(
        bucket=state_config.state_file.bucket_name,
        object_key=state_config.state_file.object_name,
        data_object=start_timestamp.isoformat()
    )


def load_state_or_default_from_s3_for_config(
    state_config: BigQueryToOpenSearchStateConfig
) -> datetime:
    try:
        return datetime.fromisoformat(
            download_s3_object_as_string_or_file_not_found_error(
                bucket=state_config.state_file.bucket_name,
                object_key=state_config.state_file.object_name
            )
        )
    except FileNotFoundError:
        LOGGER.info('state file not found, returning initial state')
        return state_config.initial_state.start_timestamp


def get_max_timestamp_from_documents(
    document_iterable: Iterable[dict],
    timestamp_key_path: Sequence[str]
) -> datetime:
    return max(
        get_required_value_for_key_path(document, timestamp_key_path)
        for document in document_iterable
    )


def create_or_update_index_and_load_documents_into_opensearch(
    document_iterable: Iterable[dict],
    config: BigQueryToOpenSearchConfig
):
    client = get_opensearch_client(config.target.opensearch)
    LOGGER.info('client: %r', client)
    prepare_opensearch(
        client=client,
        opensearch_target_config=config.target.opensearch
    )
    for index, batch_documents_iterable in enumerate(
        iter_batch_iterable(document_iterable, config.batch_size)
    ):
        batch_documents = list(batch_documents_iterable)
        LOGGER.info(
            '%s: processing batch %d (%d documents)',
            config.data_pipeline_id,
            1 + index,
            len(batch_documents)
        )
        load_documents_into_opensearch(
            batch_documents,
            client=client,
            opensearch_target_config=config.target.opensearch,
            field_names_for_config=config.field_names_for,
            batch_size=len(batch_documents)
        )
        timestamp = get_max_timestamp_from_documents(
            batch_documents,
            timestamp_key_path=config.field_names_for.timestamp_key_path
        )
        LOGGER.info('updating state with: %r', timestamp)
        save_state_to_s3_for_config(
            config.state,
            timestamp
        )


def fetch_documents_from_bigquery_and_load_into_opensearch(
    config: BigQueryToOpenSearchConfig
):
    LOGGER.debug('processing config: %r', config)
    start_timestamp = load_state_or_default_from_s3_for_config(config.state)
    LOGGER.info('start_timestamp: %r', start_timestamp)
    document_iterable = iter_documents_from_bigquery(config, start_timestamp=start_timestamp)
    create_or_update_index_and_load_documents_into_opensearch(
        document_iterable,
        config=config
    )


def fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(
    config_list: Sequence[BigQueryToOpenSearchConfig]
):
    for config in config_list:
        fetch_documents_from_bigquery_and_load_into_opensearch(config)
