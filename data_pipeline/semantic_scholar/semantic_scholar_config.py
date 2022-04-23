from typing import NamedTuple

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig


DEFAULT_BATCH_SIZE = 1000


class SemanticScholarConfig(NamedTuple):
    target: BigQueryTargetConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'SemanticScholarConfig':
        return SemanticScholarConfig(
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            ),
            batch_size=item_config_dict.get('batchSize') or DEFAULT_BATCH_SIZE
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'SemanticScholarConfig':
        item_config_list = config_dict['semanticScholar']
        return SemanticScholarConfig._from_item_dict(
            item_config_list[0]
        )
