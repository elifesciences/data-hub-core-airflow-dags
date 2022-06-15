from typing import NamedTuple

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig

class RelatedArticlesSourceConfig(NamedTuple):
    git_repo_url: str
    headers: dict

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'RelatedArticlesSourceConfig':
        return RelatedArticlesSourceConfig(
            git_repo_url=source_config_dict['gitRepoUrl'],
            headers=source_config_dict['headers']
        )

class RelatedArticlesConfig(NamedTuple):
    source_config: RelatedArticlesSourceConfig
    target_config: BigQueryTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'RelatedArticlesConfig':
        return RelatedArticlesConfig(
            source_config=RelatedArticlesSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            )
        )
