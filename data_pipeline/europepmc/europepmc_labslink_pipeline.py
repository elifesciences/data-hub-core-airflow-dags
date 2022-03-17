import logging
from typing import Sequence

from data_pipeline.europepmc.europepmc_labslink_config import (
    BigQuerySourceConfig,
    EuropePmcLabsLinkConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_article_dois_from_bigquery(
    bigquery_source: BigQuerySourceConfig
) -> Sequence[str]:
    LOGGER.debug('bigquery_source: %r', bigquery_source)
    return []


def fetch_article_dois_from_bigquery_and_update_labslink_ftp(
    config: EuropePmcLabsLinkConfig
):
    LOGGER.debug('config: %r', config)
    article_dois = fetch_article_dois_from_bigquery(config.source.bigquery)
    LOGGER.debug('article_dois: %r', article_dois)
