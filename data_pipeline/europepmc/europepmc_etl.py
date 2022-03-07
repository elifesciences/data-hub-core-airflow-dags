import logging
from typing import Iterable

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig, EuropePmcSourceConfig


LOGGER = logging.getLogger(__name__)


def iter_article_data(
    source_config: EuropePmcSourceConfig
) -> Iterable[dict]:
    LOGGER.info('source_config: %r', source_config)
    return [{'dummy': 'data'}]


def fetch_article_data_from_europepmc_and_load_into_bigquery(
    config: EuropePmcConfig
):
    data_iterable = iter_article_data(config.source)
    for data in data_iterable:
        LOGGER.info('data: %r', data)
