from typing import Mapping, NamedTuple, Optional

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)


DEFAULT_BATCH_SIZE = 1000


class SemanticScholarMatrixVariableSourceConfig(NamedTuple):
    bigquery: BigQuerySourceConfig

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'SemanticScholarMatrixVariableSourceConfig':
        return SemanticScholarMatrixVariableSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            )
        )


class SemanticScholarMatrixVariableConfig(NamedTuple):
    include: SemanticScholarMatrixVariableSourceConfig
    exclude:  Optional[SemanticScholarMatrixVariableSourceConfig] = None

    @staticmethod
    def from_dict(matrix_variable_config_dict: dict) -> 'SemanticScholarMatrixVariableConfig':
        exclude:  Optional[SemanticScholarMatrixVariableSourceConfig] = None
        if matrix_variable_config_dict.get('exclude'):
            exclude = SemanticScholarMatrixVariableSourceConfig.from_dict(
                matrix_variable_config_dict['exclude']
            )
        return SemanticScholarMatrixVariableConfig(
            include=SemanticScholarMatrixVariableSourceConfig.from_dict(
                matrix_variable_config_dict['include']
            ),
            exclude=exclude
        )


class SemanticScholarMatrixConfig(NamedTuple):
    variables: Mapping[str, SemanticScholarMatrixVariableConfig]

    @staticmethod
    def from_dict(matrix_config_dict: dict) -> 'SemanticScholarMatrixConfig':
        return SemanticScholarMatrixConfig(
            variables={
                name: SemanticScholarMatrixVariableConfig.from_dict(
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
    def from_dict(source_config_dict: dict) -> 'SemanticScholarSourceConfig':
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
    def _from_item_dict(item_config_dict: dict) -> 'SemanticScholarConfig':
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
            batch_size=item_config_dict.get('batchSize') or DEFAULT_BATCH_SIZE
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'SemanticScholarConfig':
        item_config_list = config_dict['semanticScholar']
        return SemanticScholarConfig._from_item_dict(
            item_config_list[0]
        )
