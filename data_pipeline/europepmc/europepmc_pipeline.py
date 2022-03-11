import logging
from json.decoder import JSONDecodeError
from typing import Iterable, Optional, Sequence

import requests

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig, EuropePmcSourceConfig
from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)


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


def get_filtered_article_data(data: dict, fields_to_return: Optional[Sequence[str]]) -> dict:
    if not fields_to_return:
        return data
    return {key: value for key, value in data.items() if key in fields_to_return}


def iter_article_data(
    source_config: EuropePmcSourceConfig
) -> Iterable[dict]:
    LOGGER.info('source_config: %r', source_config)
    response_json = get_article_response_json_from_api(source_config)
    return (
        get_filtered_article_data(data, source_config.fields_to_return)
        for data in iter_article_data_from_response_json(response_json)
    )


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig,
    batch_size: int = 1000
):
    data_iterable = iter_article_data(config.source)
    for batch_data_iterable in iter_batches_iterable(data_iterable, batch_size):
        batch_data_list = list(batch_data_iterable)
        LOGGER.info('batch_data_list: %r', batch_data_list)
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_list
        )
