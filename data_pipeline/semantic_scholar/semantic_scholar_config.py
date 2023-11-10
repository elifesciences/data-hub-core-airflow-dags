from typing import Mapping, NamedTuple

from data_pipeline.utils.pipeline_config import (
    BigQueryIncludeExcludeSourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_config_typing import (
    SemanticScholarConfigDict,
    SemanticScholarItemConfigDict,
    SemanticScholarMatrixConfigDict,
    SemanticScholarSourceConfigDict
)


DEFAULT_BATCH_SIZE = 1000


class SemanticScholarMatrixConfig(NamedTuple):
    variables: Mapping[str, BigQueryIncludeExcludeSourceConfig]

    @staticmethod
    def from_dict(
        matrix_config_dict: SemanticScholarMatrixConfigDict
    ) -> 'SemanticScholarMatrixConfig':
        return SemanticScholarMatrixConfig(
            variables={
                name: BigQueryIncludeExcludeSourceConfig.from_dict(
                    matrix_variable_config_dict
                )
                for name, matrix_variable_config_dict in matrix_config_dict.items()
            }
        )


class SemanticScholarSourceConfig(NamedTuple):
    api_url: str
    params: Mapping[str, str]
    headers: MappingConfig = MappingConfig.from_dict({})

    @staticmethod
    def from_dict(
        source_config_dict: SemanticScholarSourceConfigDict
    ) -> 'SemanticScholarSourceConfig':
        return SemanticScholarSourceConfig(
            api_url=source_config_dict['apiUrl'],
            params=source_config_dict.get('params', {}),
            headers=MappingConfig.from_dict(source_config_dict.get('headers', {}))
        )


class SemanticScholarConfig(NamedTuple):
    matrix: SemanticScholarMatrixConfig
    source: SemanticScholarSourceConfig
    target: BigQueryTargetConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(
        item_config_dict: SemanticScholarItemConfigDict
    ) -> 'SemanticScholarConfig':
        return SemanticScholarConfig(
            matrix=SemanticScholarMatrixConfig.from_dict(
                item_config_dict['matrix']
            ),
            source=SemanticScholarSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            ),
            batch_size=item_config_dict.get('batchSize', DEFAULT_BATCH_SIZE)
        )

    @staticmethod
    def from_dict(config_dict: SemanticScholarConfigDict) -> 'SemanticScholarConfig':
        item_config_list = config_dict['semanticScholar']
        return SemanticScholarConfig._from_item_dict(
            item_config_list[0]
        )
