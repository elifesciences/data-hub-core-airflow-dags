import logging
from dataclasses import dataclass, field
from typing import Optional, Sequence

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
class OpenSearchTargetConfig:  # pylint: disable=too-many-instance-attributes
    hostname: str
    port: int
    username: str = field(repr=False)
    password: str = field(repr=False)
    index_name: str
    update_index_settings: bool = False
    update_mappings: bool = False
    index_settings: Optional[dict] = None
    verify_certificates: bool = True

    @staticmethod
    def from_dict(opensearch_target_config_dict: dict) -> 'OpenSearchTargetConfig':
        secrets = get_resolved_parameter_values_from_file_path_env_name(
            opensearch_target_config_dict['secrets']['parametersFromFile']
        )
        return OpenSearchTargetConfig(
            hostname=opensearch_target_config_dict['hostname'],
            port=opensearch_target_config_dict['port'],
            username=secrets['username'],
            password=secrets['password'],
            index_name=opensearch_target_config_dict['indexName'],
            update_index_settings=opensearch_target_config_dict.get('updateIndexSettings', False),
            update_mappings=opensearch_target_config_dict.get('updateMappings', False),
            index_settings=opensearch_target_config_dict.get('indexSettings'),
            verify_certificates=opensearch_target_config_dict.get('verifyCertificates', True)
        )


@dataclass(frozen=True)
class BigQueryToOpenSearchFieldNamesForConfig:
    id: str  # pylint: disable=invalid-name
    timestamp: str

    @staticmethod
    def from_dict(field_names_for_config_dict: dict) -> 'BigQueryToOpenSearchFieldNamesForConfig':
        return BigQueryToOpenSearchFieldNamesForConfig(
            id=field_names_for_config_dict['id'],
            timestamp=field_names_for_config_dict['timestamp']
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


DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True)
class BigQueryToOpenSearchConfig:
    source: BigQueryToOpenSearchSourceConfig
    field_names_for: BigQueryToOpenSearchFieldNamesForConfig
    target: BigQueryToOpenSearchTargetConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'BigQueryToOpenSearchConfig':
        return BigQueryToOpenSearchConfig(
            source=BigQueryToOpenSearchSourceConfig.from_dict(item_config_dict['source']),
            field_names_for=BigQueryToOpenSearchFieldNamesForConfig.from_dict(
                item_config_dict['fieldNamesFor']
            ),
            target=BigQueryToOpenSearchTargetConfig.from_dict(item_config_dict['target']),
            batch_size=item_config_dict.get('batchSize', DEFAULT_BATCH_SIZE)
        )

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['BigQueryToOpenSearchConfig']:
        LOGGER.debug('config_dict: %r', config_dict)
        item_config_dict_list = config_dict['bigQueryToOpenSearch']
        return [
            BigQueryToOpenSearchConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
