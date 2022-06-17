from typing import NamedTuple

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig


class ElifeArticleXmlSourceConfig(NamedTuple):
    git_repo_url: str
    directory_name: str

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'ElifeArticleXmlSourceConfig':
        return ElifeArticleXmlSourceConfig(
            git_repo_url=source_config_dict['gitRepoUrl'],
            directory_name=source_config_dict['directoryName']
        )


class ElifeArticleXmlConfig(NamedTuple):
    source: ElifeArticleXmlSourceConfig
    target: BigQueryTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'ElifeArticleXmlConfig':
        return ElifeArticleXmlConfig(
            source=ElifeArticleXmlSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'ElifeArticleXmlConfig':
        item_config_list = config_dict['elifeArticleXml']
        return ElifeArticleXmlConfig._from_item_dict(
            item_config_list[0]
        )
