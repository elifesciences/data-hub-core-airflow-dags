import logging
from functools import partial
from typing import Iterable, Optional

from data_pipeline.utils.collections import iter_batches_iterable
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


LOGGER = logging.getLogger(__name__)


def iter_doi_for_matrix_config(matrix_config: SemanticScholarMatrixConfig) -> Iterable[str]:
    return fetch_single_column_value_list_for_bigquery_source_config(
        matrix_config.variables['doi'].include.bigquery
    )


def get_resolved_api_url(api_url: str, **kwargs) -> str:
    return api_url.format(**kwargs)


def get_request_params_for_source_config(
    source_config: SemanticScholarSourceConfig
) -> dict:
    return source_config.params


def get_article_response_json_from_api(
    doi: str,
    source_config: SemanticScholarSourceConfig,
    provenance: Optional[dict] = None
) -> dict:
    url = get_resolved_api_url(
        source_config.api_url,
        doi=doi
    )
    params = get_request_params_for_source_config(source_config)
    LOGGER.debug('resolved url: %r (%r)', url, params)
    return get_response_json_with_provenance_from_api(
        url,
        params=params,
        provenance=provenance
    )


def iter_article_data(
    doi_iterable: Iterable[str],
    source_config: SemanticScholarSourceConfig
) -> Iterable[dict]:
    fetch_article_by_doi = partial(
        get_article_response_json_from_api,
        source_config=source_config
    )
    return map(fetch_article_by_doi, doi_iterable)


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
    config: SemanticScholarConfig
):
    LOGGER.info('config: %r', config)
    batch_size = config.batch_size
    doi_iterable = iter_doi_for_matrix_config(config.matrix)
    data_iterable = iter_article_data(
        doi_iterable,
        source_config=config.source
    )
    for batch_data_iterable in iter_batches_iterable(data_iterable, batch_size):
        batch_data_list = list(batch_data_iterable)
        LOGGER.debug('batch_data_list: %r', batch_data_list)
        LOGGER.info('loading batch into bigquery: %d', len(batch_data_list))
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_list
        )
