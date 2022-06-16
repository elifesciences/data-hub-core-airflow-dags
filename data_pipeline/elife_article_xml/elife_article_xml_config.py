from typing import NamedTuple

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig


class RelatedArticlesSourceConfig(NamedTuple):
    git_repo_url: str
    directory_name: str

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'RelatedArticlesSourceConfig':
        return RelatedArticlesSourceConfig(
            git_repo_url=source_config_dict['gitRepoUrl'],
            directory_name=source_config_dict['directoryName']
        )


class RelatedArticlesConfig(NamedTuple):
    source: RelatedArticlesSourceConfig
    target: BigQueryTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'RelatedArticlesConfig':
        return RelatedArticlesConfig(
            source=RelatedArticlesSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'RelatedArticlesConfig':
        item_config_list = config_dict['elifeArticleXml']
        return RelatedArticlesConfig._from_item_dict(
            item_config_list[0]
        )
