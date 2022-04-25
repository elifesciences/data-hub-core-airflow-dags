import logging
from typing import Iterable, Sequence

from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    get_single_column_value_list_from_bq_query,
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarConfig,
    SemanticScholarMatrixConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_article_dois_from_bigquery(
    bigquery_source_config: BigQuerySourceConfig
) -> Sequence[str]:
    LOGGER.debug('bigquery_source: %r', bigquery_source_config)
    doi_list = get_single_column_value_list_from_bq_query(
        project_name=bigquery_source_config.project_name,
        query=bigquery_source_config.sql_query
    )
    LOGGER.debug('doi_list: %r', doi_list)
    LOGGER.info('length of doi_list: %r', len(doi_list))
    return doi_list


def iter_doi_for_matrix_config(matrix_config: SemanticScholarMatrixConfig) -> Iterable[str]:
    return fetch_article_dois_from_bigquery(
        matrix_config.variables['doi'].include.bigquery
    )


def fetch_article_by_doi(doi: str) -> dict:
    return {'doi': doi}


def iter_article_data(doi_iterable: Iterable[str]) -> Iterable[dict]:
    return map(fetch_article_by_doi, doi_iterable)


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
    config: SemanticScholarConfig
):
    LOGGER.info('config: %r', config)
    batch_size = config.batch_size
    doi_iterable = iter_doi_for_matrix_config(config.matrix)
    data_iterable = iter_article_data(doi_iterable)
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
