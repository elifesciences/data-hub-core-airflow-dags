import logging
from dataclasses import dataclass, field
from typing import Sequence

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    get_resolved_parameter_values_from_file_path_env_name
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BigQueryToOpenSearchSourceConfig:
    bigquery: BigQuerySourceConfig

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'BigQueryToOpenSearchSourceConfig':
        return BigQueryToOpenSearchSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            )
        )


@dataclass(frozen=True)
class OpenSearchTargetConfig:
    hostname: str
    port: int
    username: str = field(repr=False)
    password: str = field(repr=False)

    @staticmethod
    def from_dict(opensearch_target_config_dict: dict) -> 'OpenSearchTargetConfig':
        secrets = get_resolved_parameter_values_from_file_path_env_name(
            opensearch_target_config_dict['secrets']['parametersFromFile']
        )
        return OpenSearchTargetConfig(
            hostname=opensearch_target_config_dict['hostname'],
            port=opensearch_target_config_dict['port'],
            username=secrets['username'],
            password=secrets['password']
        )


@dataclass(frozen=True)
class BigQueryToOpenSearchTargetConfig:
    opensearch: OpenSearchTargetConfig

    @staticmethod
    def from_dict(target_config_dict: dict) -> 'BigQueryToOpenSearchTargetConfig':
        return BigQueryToOpenSearchTargetConfig(
            opensearch=OpenSearchTargetConfig.from_dict(
                target_config_dict['opensearch']
            )
        )


@dataclass(frozen=True)
class BigQueryToOpenSearchConfig:
    source: BigQueryToOpenSearchSourceConfig
    target: BigQueryToOpenSearchTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'BigQueryToOpenSearchConfig':
        return BigQueryToOpenSearchConfig(
            source=BigQueryToOpenSearchSourceConfig.from_dict(item_config_dict['source']),
            target=BigQueryToOpenSearchTargetConfig.from_dict(item_config_dict['target'])
        )

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['BigQueryToOpenSearchConfig']:
        LOGGER.debug('config_dict: %r', config_dict)
        item_config_dict_list = config_dict['bigQueryToOpenSearch']
        return [
            BigQueryToOpenSearchConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
