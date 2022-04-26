import logging
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Mapping, Optional, Sequence

import requests

import google.cloud.exceptions

from data_pipeline.utils.data_store.bq_data_service import (
    get_single_column_value_list_from_bq_query
)
from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_single_column_value_list_for_bigquery_source_config(
    bigquery_source_config: BigQuerySourceConfig
) -> Sequence[str]:
    LOGGER.debug('bigquery_source: %r', bigquery_source_config)
    try:
        doi_list = get_single_column_value_list_from_bq_query(
            project_name=bigquery_source_config.project_name,
            query=bigquery_source_config.sql_query
        )
    except google.cloud.exceptions.NotFound:
        if bigquery_source_config.ignore_not_found:
            LOGGER.info('caught not found, returning empty list')
            return []
        raise
    LOGGER.debug('doi_list: %r', doi_list)
    LOGGER.info('length of doi_list: %r', len(doi_list))
    return doi_list


def get_valid_json_from_response(response: requests.Response) -> dict:
    try:
        response.raise_for_status()
        return response.json()
    except JSONDecodeError:
        LOGGER.warning('failed to decode json: %r', response.text)
        raise


def get_response_json_with_provenance_from_api(
    url: str,
    params: Mapping[str, str] = None,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None
) -> dict:
    LOGGER.info('requesting url: %r (%r)', url, params)
    request_timestamp = datetime.utcnow()
    if session:
        response = session.get(url, params=params)
    else:
        response = requests.get(url, params=params)
    response_timestamp = datetime.utcnow()
    response_duration_secs = (response_timestamp - request_timestamp).total_seconds()
    LOGGER.info('request took: %0.3f seconds', response_duration_secs)
    request_provenance = {
        **(provenance or {}),
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
    return {
        **get_valid_json_from_response(response),
        'provenance': request_provenance
    }
