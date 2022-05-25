from typing import NamedTuple

from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarMatrixConfig,
    SemanticScholarSourceConfig
)
from data_pipeline.utils.pipeline_config import BigQueryTargetConfig


DEFAULT_BATCH_SIZE = 1000


class SemanticScholarRecommendationConfig(NamedTuple):
    matrix: SemanticScholarMatrixConfig
    source: SemanticScholarSourceConfig
    target: BigQueryTargetConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'SemanticScholarRecommendationConfig':
        return SemanticScholarRecommendationConfig(
            matrix=SemanticScholarMatrixConfig.from_dict(
                item_config_dict['matrix']
            ),
            source=SemanticScholarSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            ),
            batch_size=item_config_dict.get('batchSize') or DEFAULT_BATCH_SIZE
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'SemanticScholarRecommendationConfig':
        item_config_list = config_dict['semanticScholarRecommendation']
        return SemanticScholarRecommendationConfig._from_item_dict(
            item_config_list[0]
        )
