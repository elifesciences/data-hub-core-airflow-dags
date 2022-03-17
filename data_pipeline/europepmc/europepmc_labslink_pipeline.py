import logging

from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_article_dois_from_bigquery_and_update_labslink_ftp(
    config: EuropePmcLabsLinkConfig
):
    LOGGER.debug('config: %r', config)
