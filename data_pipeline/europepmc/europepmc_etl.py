import logging
from json.decoder import JSONDecodeError
from typing import Iterable

import requests

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig, EuropePmcSourceConfig


LOGGER = logging.getLogger(__name__)


def iter_article_data_from_response_json(
    response_json: dict
) -> Iterable[dict]:
    return response_json['resultList']['result']


def get_request_params_for_source_config(
    source_config: EuropePmcSourceConfig
) -> dict:
    return {
        'query': source_config.search.query,
        'format': 'json',
        'resultType': 'core'
    }


def get_valid_json_from_response(response: requests.Response) -> dict:
    try:
        response.raise_for_status()
        return response.json()
    except JSONDecodeError:
        LOGGER.warning('failed to decode json: %r', response.text)
        raise


def get_article_response_json_from_api(
    source_config: EuropePmcSourceConfig
) -> dict:
    url = source_config.api_url
    params = get_request_params_for_source_config(source_config)
    response = requests.get(url, params=params)
    return get_valid_json_from_response(response)


def iter_article_data(
    source_config: EuropePmcSourceConfig
) -> Iterable[dict]:
    LOGGER.info('source_config: %r', source_config)
    response_json = get_article_response_json_from_api(source_config)
    yield from iter_article_data_from_response_json(response_json)


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig
):
    data_iterable = iter_article_data(config.source)
    for data in data_iterable:
        LOGGER.info('data: %r', data)
