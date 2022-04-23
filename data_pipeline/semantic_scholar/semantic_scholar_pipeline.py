import logging

from data_pipeline.semantic_scholar.semantic_scholar_config import SemanticScholarConfig


LOGGER = logging.getLogger(__name__)


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
    config: SemanticScholarConfig
):
    LOGGER.info('config: %r', config)
