import logging

from data_pipeline.semantic_scholar.semantic_scholar_recommendation_config import (
    SemanticScholarRecommendationConfig
)


LOGGER = logging.getLogger(__name__)


def fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
    config: SemanticScholarRecommendationConfig
):
    LOGGER.info('config: %r', config)
