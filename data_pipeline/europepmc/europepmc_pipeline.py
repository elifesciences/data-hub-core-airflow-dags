import logging
from datetime import date, timedelta
from json.decoder import JSONDecodeError
from typing import Iterable, Optional, Sequence

import requests

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig, EuropePmcInitialStateConfig, EuropePmcSourceConfig, EuropePmcStateConfig
from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.data_store.s3_data_service import upload_s3_object


LOGGER = logging.getLogger(__name__)


def iter_article_data_from_response_json(
    response_json: dict
) -> Iterable[dict]:
    return response_json['resultList']['result']


def get_request_query_for_source_config_and_initial_state(
    source_config: EuropePmcSourceConfig,
    initial_state_config: EuropePmcInitialStateConfig
) -> str:
    query = f"(FIRST_IDATE:'{initial_state_config.start_date_str}') " + source_config.search.query
    return query


def get_request_params_for_source_config(
    source_config: EuropePmcSourceConfig,
    initial_state_config: EuropePmcInitialStateConfig
) -> dict:
    return {
        'query': get_request_query_for_source_config_and_initial_state(
            source_config,
            initial_state_config
        ),
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
    source_config: EuropePmcSourceConfig,
    initial_state_config: EuropePmcInitialStateConfig
) -> dict:
    url = source_config.api_url
    params = get_request_params_for_source_config(
        source_config,
        initial_state_config
    )
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


def save_state_for_config(
    state_config: EuropePmcStateConfig
):
    parsed_date = date.fromisoformat(state_config.initial_state.start_date_str)
    next_day_date = parsed_date + timedelta(days=1)
    upload_s3_object(
        bucket=state_config.state_file.bucket_name,
        object_key=state_config.state_file.object_name,
        data_object=next_day_date.isoformat()
    )


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig
):
    batch_size = config.batch_size
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
    save_state_for_config(config.state)
