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


def iter_article_data(
    source_config: EuropePmcSourceConfig
) -> Iterable[dict]:
    LOGGER.info('source_config: %r', source_config)
    url = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
    response = requests.get(url, params={
        'query': source_config.search.query,
        'format': 'json',
        'resultType': 'core'
    })
    try:
        response.raise_for_status()
        yield from iter_article_data_from_response_json(response.json())
    except JSONDecodeError:
        LOGGER.warning('failed to decode: %r', response.text)
        raise


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig
):
    data_iterable = iter_article_data(config.source)
    for data in data_iterable:
        LOGGER.info('data: %r', data)
