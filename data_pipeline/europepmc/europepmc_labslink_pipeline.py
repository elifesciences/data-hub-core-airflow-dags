import logging
from typing import Sequence

from data_pipeline.europepmc.europepmc_labslink_config import (
    BigQuerySourceConfig,
    EuropePmcLabsLinkConfig
)
from data_pipeline.utils.data_store.bq_data_service import (
    get_single_column_value_list_from_bq_query
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


def fetch_article_dois_from_bigquery_and_update_labslink_ftp(
    config: EuropePmcLabsLinkConfig
):
    LOGGER.debug('config: %r', config)
    article_dois = fetch_article_dois_from_bigquery(config.source.bigquery)
    LOGGER.debug('article_dois: %r', article_dois)
