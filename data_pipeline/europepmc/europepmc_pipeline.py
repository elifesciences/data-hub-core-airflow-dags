import logging
from datetime import date, datetime, timedelta
from typing import Iterable, List, NamedTuple, Optional, Sequence

from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig,
    EuropePmcSourceConfig,
    EuropePmcStateConfig
)
from data_pipeline.utils.collections import iter_batch_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    upload_s3_object
)
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.pipeline_utils import get_response_json_with_provenance_from_api


LOGGER = logging.getLogger(__name__)


DEFAULT_CURSOR = '*'


class EuropePmcSearchContext(NamedTuple):
    start_date_str: str
    end_date_str: str

    def is_empty_period(self):
        return date.fromisoformat(self.end_date_str) < date.fromisoformat(self.start_date_str)


def iter_article_data_from_response_json(
    response_json: dict
) -> Iterable[dict]:
    provenance = response_json.get('provenance', {})
    return (
        {
            **item,
            'provenance': provenance
        }
        for item in response_json['resultList']['result']
    )


def get_request_query_for_source_config_and_start_date_str(
    source_config: EuropePmcSourceConfig,
    search_context: EuropePmcSearchContext
) -> str:
    date_period_str = (
        f'[{search_context.start_date_str} TO {search_context.end_date_str}]'
    )
    query = f'(FIRST_IDATE:{date_period_str})'
    if source_config.search.query:
        query += ' (' + source_config.search.query + ')'
    return query


def get_request_params_for_source_config(
    source_config: EuropePmcSourceConfig,
    search_context: EuropePmcSearchContext,
    cursor: Optional[str] = DEFAULT_CURSOR
) -> dict:
    return {
        **(source_config.search.extra_params or {}),
        'query': get_request_query_for_source_config_and_start_date_str(
            source_config,
            search_context
        ),
        'cursorMark': cursor,
        'format': 'json',
        'resultType': 'core'
    }


def get_article_response_json_from_api(
    source_config: EuropePmcSourceConfig,
    search_context: EuropePmcSearchContext,
    cursor: Optional[str] = DEFAULT_CURSOR,
    provenance: Optional[dict] = None
) -> dict:
    url = source_config.api_url
    params = get_request_params_for_source_config(
        source_config,
        search_context,
        cursor=cursor
    )
    return get_response_json_with_provenance_from_api(
        url,
        params=params,
        provenance=provenance
    )


def get_requested_fields_of_the_article_data(
    data: dict,
    fields_to_return: Optional[Sequence[str]]
) -> dict:
    if not fields_to_return:
        return data
    fields_to_return_set = set(fields_to_return).union({'provenance'})
    LOGGER.debug('fields_to_return_set: %r', fields_to_return_set)
    LOGGER.debug('data.keys: %r', data.keys())
    return {key: value for key, value in data.items() if key in fields_to_return_set}


def get_next_cursor_from_response_json(response_json: dict) -> Optional[str]:
    return response_json.get('nextCursorMark')


def iter_article_data(
    source_config: EuropePmcSourceConfig,
    search_context: EuropePmcSearchContext,
    provenance: Optional[dict] = None
) -> Iterable[dict]:
    LOGGER.info('source_config: %r', source_config)
    cursor: Optional[str] = DEFAULT_CURSOR
    while cursor:
        response_json = get_article_response_json_from_api(
            source_config,
            search_context,
            cursor=cursor,
            provenance=provenance
        )
        next_cursor = get_next_cursor_from_response_json(response_json)
        LOGGER.debug('response_json (next cursor: %r): %r', next_cursor, response_json)
        if source_config.extract_individual_results_from_response:
            for data in iter_article_data_from_response_json(response_json):
                yield get_requested_fields_of_the_article_data(data, source_config.fields_to_return)
        else:
            yield response_json
        if next_cursor == cursor:
            LOGGER.warning('ignoring next cursor, same as previous cursor: %r', next_cursor)
            break
        cursor = next_cursor


def save_state_to_s3_for_config(
    state_config: EuropePmcStateConfig,
    start_date_str: str
):
    upload_s3_object(
        bucket=state_config.state_file.bucket_name,
        object_key=state_config.state_file.object_name,
        data_object=start_date_str
    )


def get_next_start_date_str_for_end_date_str(
    end_date_str: str
):
    end_date = date.fromisoformat(end_date_str)
    next_start_date = end_date + timedelta(days=1)
    return next_start_date.isoformat()


def get_search_context_for_start_date_str(
    start_date_str: str,
    max_days: Optional[int] = None
) -> EuropePmcSearchContext:
    today = date.today()
    yesterday = today - timedelta(days=1)
    if not max_days:
        end_date = yesterday
    else:
        end_date = date.fromisoformat(start_date_str) + timedelta(days=max_days - 1)
        if end_date >= today:
            end_date = yesterday
    end_date_str = end_date.isoformat()
    return EuropePmcSearchContext(
        start_date_str=start_date_str,
        end_date_str=end_date_str
    )


def load_state_from_s3_for_config(
    state_config: EuropePmcStateConfig
):
    try:
        return download_s3_object_as_string_or_file_not_found_error(
            bucket=state_config.state_file.bucket_name,
            object_key=state_config.state_file.object_name
        )
    except FileNotFoundError:
        LOGGER.info('state file not found, returning initial state')
        return state_config.initial_state.start_date_str


def get_latest_index_date_from_article_data(
    article_data: dict,
    source_config: EuropePmcSourceConfig
) -> Optional[date]:
    if source_config.extract_individual_results_from_response:
        item_list = [article_data]
    else:
        item_list = article_data['resultList']['result']
    if not item_list:
        return None
    return max((
        date.fromisoformat(item['firstIndexDate'])
        for item in item_list
    ))


def iter_article_data_for_bq_and_collect_latest_index_dates(
    data_iterable: Iterable[dict],
    source_config: EuropePmcSourceConfig,
    latest_index_date_list: List[date]
) -> Iterable[dict]:
    for item in data_iterable:
        latest_index_date = get_latest_index_date_from_article_data(item, source_config)
        if latest_index_date:
            latest_index_date_list.append(latest_index_date)
        yield remove_key_with_null_value(item)


def fetch_article_data_and_load_into_bq_for_search_context_and_return_latest_index_date(
    config: EuropePmcConfig,
    search_context: EuropePmcSearchContext
) -> Optional[date]:
    batch_size = config.batch_size
    provenance = {'imported_timestamp': datetime.utcnow().isoformat()}
    data_iterable = iter_article_data(
        config.source,
        search_context,
        provenance=provenance
    )
    latest_index_date_list: List = []
    for batch_data_iterable in iter_batch_iterable(data_iterable, batch_size):
        batch_data_iterable = iter_article_data_for_bq_and_collect_latest_index_dates(
            data_iterable=batch_data_iterable,
            source_config=config.source,
            latest_index_date_list=latest_index_date_list
        )
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_iterable
        )
    if latest_index_date_list:
        return max(latest_index_date_list)
    return None


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig
):
    LOGGER.info('processing config: %r', config)
    start_date_str = load_state_from_s3_for_config(
        config.state
    )
    while True:
        search_context = get_search_context_for_start_date_str(
            start_date_str,
            max_days=config.source.max_days
        )
        LOGGER.info('search_context: %r', search_context)
        if search_context.is_empty_period():
            LOGGER.info('empty period, skip processing')
            return
        latest_index_date = (
            fetch_article_data_and_load_into_bq_for_search_context_and_return_latest_index_date(
                config,
                search_context=search_context
            )
        )
        LOGGER.debug('latest_index_date: %r', latest_index_date)
        next_start_date_str = get_next_start_date_str_for_end_date_str(
            search_context.end_date_str
        )
        if latest_index_date:
            assert latest_index_date >= date.fromisoformat(search_context.start_date_str)
            assert latest_index_date <= date.fromisoformat(search_context.end_date_str)
            save_state_to_s3_for_config(
                config.state,
                (latest_index_date + timedelta(days=1)).isoformat()
            )
        start_date_str = next_start_date_str


def fetch_article_data_from_europepmc_and_load_into_bigquery_from_config_list(
    config_list: Sequence[EuropePmcConfig]
):
    for config in config_list:
        fetch_article_data_from_europepmc_and_load_into_bigquery(config)
