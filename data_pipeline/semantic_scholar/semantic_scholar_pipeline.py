import logging
from datetime import datetime
from typing import Iterable, Mapping, Optional

import requests

from data_pipeline.utils.collections import iter_batch_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.pipeline_utils import (
    fetch_single_column_value_list_for_bigquery_source_config,
    get_response_json_with_provenance_from_api
)
from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarConfig,
    SemanticScholarMatrixConfig,
    SemanticScholarSourceConfig
)
from data_pipeline.utils.web_api import requests_retry_session


LOGGER = logging.getLogger(__name__)


def iter_doi_for_matrix_config(matrix_config: SemanticScholarMatrixConfig) -> Iterable[str]:
    variable_config = matrix_config.variables['doi']
    include_list = fetch_single_column_value_list_for_bigquery_source_config(
        variable_config.include.bigquery
    )
    if not include_list:
        LOGGER.info('empty include list')
        return include_list
    if not variable_config.exclude:
        LOGGER.info('found %d include item (no exclude config)', len(include_list))
        return include_list
    exclude_list = fetch_single_column_value_list_for_bigquery_source_config(
        variable_config.exclude.bigquery
    )
    if not exclude_list:
        LOGGER.info('found %d include item (empty exclude list)', len(include_list))
        return include_list
    result = sorted(set(include_list) - set(exclude_list))
    LOGGER.info(
        'found %d items (include:%d, exclude:%d)',
        len(result), len(include_list), len(exclude_list)
    )
    return result


def get_resolved_api_url(api_url: str, **kwargs) -> str:
    return api_url.format(**kwargs)


def get_request_params_for_source_config(
    source_config: SemanticScholarSourceConfig
) -> dict:
    return source_config.params


def get_article_response_json_from_api(
    doi: str,
    source_config: SemanticScholarSourceConfig,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None,
    progress_message: Optional[str] = None
) -> dict:
    url = get_resolved_api_url(
        source_config.api_url,
        doi=doi
    )
    params = get_request_params_for_source_config(source_config)
    LOGGER.debug('resolved url: %r (%r)', url, params)
    extended_provenance = {
        **(provenance or {}),
        'doi': doi
    }
    return get_response_json_with_provenance_from_api(
        url,
        params=params,
        headers=source_config.headers.mapping,
        printable_headers=source_config.headers.printable_mapping,
        provenance=extended_provenance,
        session=session,
        raise_on_status=False,
        progress_message=progress_message
    )


def get_progress_message(index: int, iterable):
    try:
        total = len(iterable)
        return f'{1 + index}/{total}'
    except TypeError:
        return f'{1 + index}'


def iter_article_data(
    doi_iterable: Iterable[str],
    source_config: SemanticScholarSourceConfig,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None
) -> Iterable[dict]:
    for index, doi in enumerate(doi_iterable):
        progress_message = get_progress_message(index, doi_iterable)
        yield get_article_response_json_from_api(
            doi,
            source_config=source_config,
            provenance=provenance,
            session=session,
            progress_message=progress_message
        )


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
    config: SemanticScholarConfig
):
    LOGGER.info('config: %r', config)
    batch_size = config.batch_size
    provenance = {'imported_timestamp': datetime.utcnow().isoformat()}
    doi_iterable = iter_doi_for_matrix_config(config.matrix)
    with requests_retry_session(
        status_forcelist=(500, 502, 503, 504, 429),
        raise_on_redirect=False,  # avoid raising exception, instead we will save response as is
        raise_on_status=False
    ) as session:
        data_iterable = iter_article_data(
            doi_iterable,
            source_config=config.source,
            provenance=provenance,
            session=session
        )
    for batch_data_iterable in iter_batch_iterable(data_iterable, batch_size):
        batch_data_list = list(batch_data_iterable)
        LOGGER.debug('batch_data_list: %r', batch_data_list)
        LOGGER.info('loading batch into bigquery: %d', len(batch_data_list))
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_list
        )
