import argparse
import logging
from typing import Optional, Sequence

from data_pipeline.semantic_scholar.semantic_scholar_recommendation_config import (
    SemanticScholarRecommendationConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_recommendation_pipeline import (
    fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


class SemanticScholarPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'SEMANTIC_SCHOLAR_RECOMMENDATION_CONFIG_FILE_PATH'


def get_pipeline_config() -> 'SemanticScholarRecommendationConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        SemanticScholarPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        SemanticScholarRecommendationConfig.from_dict
    )


def main(argv: Optional[Sequence[str]] = None):
    # Name CLI and declare no arguments
    parser = argparse.ArgumentParser(
        description='Run ETL for a Semantic Scholar Recommendation pipeline'
    )
    parser.parse_args(argv)

    pipeline_config = get_pipeline_config()
    LOGGER.info('pipeline_config: %r', pipeline_config)

    LOGGER.info('Starting ETL')
    fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
        pipeline_config
    )
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
