import logging
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Any, Iterable, Mapping, Optional, Sequence

import requests

import google.cloud.exceptions

from data_pipeline.utils.data_store.bq_data_service import (
    get_query_with_exclusion,
    get_single_column_value_list_from_bq_query,
    iter_dict_from_bq_query
)
from data_pipeline.utils.pipeline_config import (
    SECRET_VALUE_PLACEHOLDER,
    BigQuerySourceConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_single_column_value_list_for_bigquery_source_config(
    bigquery_source_config: BigQuerySourceConfig
) -> Sequence[str]:
    LOGGER.debug('bigquery_source: %r', bigquery_source_config)
    try:
        value_list = get_single_column_value_list_from_bq_query(
            project_name=bigquery_source_config.project_name,
            query=bigquery_source_config.sql_query
        )
    except google.cloud.exceptions.NotFound:
        if bigquery_source_config.ignore_not_found:
            LOGGER.info('caught not found, returning empty list')
            return []
        raise
    LOGGER.debug('value_list: %r', value_list)
    LOGGER.info('length of value_list: %r', len(value_list))
    return value_list


def iter_dict_for_bigquery_source_config_with_exclusion(
    bigquery_source_config: BigQuerySourceConfig,
    key_field_name: str,
    exclude_bigquery_source_config: Optional[BigQuerySourceConfig] = None
) -> Iterable[dict]:
    LOGGER.debug('bigquery_source: %r', bigquery_source_config)
    query = bigquery_source_config.sql_query
    if exclude_bigquery_source_config:
        query_with_exclusion = get_query_with_exclusion(
            query,
            key_field_name=key_field_name,
            exclude_query=exclude_bigquery_source_config.sql_query
        )
    else:
        query_with_exclusion = query
    try:
        yield from iter_dict_from_bq_query(
            project_name=bigquery_source_config.project_name,
            query=query_with_exclusion
        )
        return
    except google.cloud.exceptions.NotFound:
        if query_with_exclusion == query and not bigquery_source_config.ignore_not_found:
            raise
        if exclude_bigquery_source_config and not exclude_bigquery_source_config.ignore_not_found:
            raise
    try:
        LOGGER.info('caught not found, attempting query without exclusion')
        yield from iter_dict_from_bq_query(
            project_name=bigquery_source_config.project_name,
            query=bigquery_source_config.sql_query
        )
    except google.cloud.exceptions.NotFound:
        if not bigquery_source_config.ignore_not_found:
            raise
        LOGGER.info('caught not found, returning empty list')


def get_valid_json_from_response(response: requests.Response) -> dict:
    try:
        return response.json()
    except JSONDecodeError:
        LOGGER.warning('failed to decode json: %r', response.text)
        raise


def get_default_printable_mapping_with_secrets(
    mapping: Optional[Mapping[str, str]] = None,
    printable_mapping: Optional[Mapping[str, str]] = None
) -> Optional[Mapping[str, str]]:
    if printable_mapping is not None:
        return printable_mapping
    if mapping is not None:
        return {key: SECRET_VALUE_PLACEHOLDER for key, value in mapping.items()}
    return None


def get_response_json_with_provenance_from_api(  # noqa pylint: disable=too-many-arguments,too-many-locals
    url: str,
    params: Mapping[str, str] = None,
    headers: Mapping[str, str] = None,
    printable_headers: Mapping[str, str] = None,
    method: str = 'GET',
    json_data: Optional[Any] = None,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None,
    raise_on_status: bool = True,
    progress_message: Optional[str] = None
) -> dict:
    progress_message_str = (
        f'({progress_message})'
        if progress_message
        else ''
    )
    printable_headers = get_default_printable_mapping_with_secrets(
        mapping=headers,
        printable_mapping=printable_headers
    )
    LOGGER.info(
        'requesting url%s: %r %r (params=%r, headers=%r)',
        progress_message_str, method, url, params, printable_headers
    )
    request_timestamp = datetime.utcnow()
    if session:
        response = session.request(method, url, params=params, headers=headers, json=json_data)
    else:
        response = requests.request(method, url, params=params, headers=headers, json=json_data)
    response_timestamp = datetime.utcnow()
    LOGGER.debug('raise_on_status: %r', raise_on_status)
    response_duration_secs = (response_timestamp - request_timestamp).total_seconds()
    LOGGER.info(
        'request took: %0.3f seconds (status_code: %r)',
        response_duration_secs, response.status_code
    )
    if raise_on_status:
        response.raise_for_status()
    request_provenance = {
        **(provenance or {}),
        'method': method,
        'api_url': url,
        'request_url': response.url,
        'http_status': response.status_code,
        'request_timestamp': request_timestamp.isoformat(),
        'response_timestamp': response_timestamp.isoformat()
    }
    if params:
        request_provenance['request_params'] = [
            {'name': key, 'value': value} for key, value in params.items()
        ]
    if printable_headers:
        assert headers, 'headers required when passing in printable_headers'
        assert printable_headers.keys() == headers.keys(), \
            'keys of printable_headers and headers do not match'
        request_provenance['request_headers'] = [
            {'name': key, 'value': value} for key, value in printable_headers.items()
        ]
    if json_data:
        request_provenance['json_data'] = json_data
    return {
        **get_valid_json_from_response(response),
        'provenance': request_provenance
    }
