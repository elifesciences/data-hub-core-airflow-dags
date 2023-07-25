from typing import NamedTuple, Sequence

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig, MappingConfig


class ElifeArticleXmlSourceConfig(NamedTuple):
    git_repo_url: str
    directory_name: str
    searched_xml_elements: Sequence[str]
    headers: MappingConfig = MappingConfig.from_dict({})

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'ElifeArticleXmlSourceConfig':
        return ElifeArticleXmlSourceConfig(
            git_repo_url=source_config_dict['gitRepoUrl'],
            directory_name=source_config_dict['directoryName'],
            headers=MappingConfig.from_dict(source_config_dict.get('headers', {})),
            searched_xml_elements=source_config_dict['searchedXmlElements']
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
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['ElifeArticleXmlConfig']:
        item_config_dict_list = config_dict['elifeArticleXml']
        return [
            ElifeArticleXmlConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
