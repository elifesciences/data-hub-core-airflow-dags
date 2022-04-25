import logging
from typing import Sequence

from data_pipeline.utils.data_store.bq_data_service import (
    get_single_column_value_list_from_bq_query
)
from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_single_column_value_list_for_bigquery_source_config(
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
