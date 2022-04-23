import logging
from typing import Iterable

from data_pipeline.semantic_scholar.semantic_scholar_config import SemanticScholarConfig
from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)


LOGGER = logging.getLogger(__name__)


def iter_article_data() -> Iterable[dict]:
    return []


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
    config: SemanticScholarConfig
):
    LOGGER.info('config: %r', config)
    batch_size = config.batch_size
    data_iterable = iter_article_data()
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
