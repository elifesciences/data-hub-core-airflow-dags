from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import Optional, Sequence

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    StateFileConfig,
    get_resolved_parameter_values_from_file_path_env_name
)


LOGGER = logging.getLogger(__name__)


# The default timeout for OpenSearch operations in seconds
DEFAULT_OPENSEARCH_TIMEOUT = 60.0


# See: https://opensearch.org/docs/latest/api-reference/document-apis/bulk/
class OpenSearchOperationModes:
    CREATE = 'create'
    DELETE = 'delete'
    INDEX = 'index'
    UPDATE = 'update'


# Note: We currently do not support the DELETE operation mode
VALID_OPENSEARCH_OPERATION_MODES = {
    OpenSearchOperationModes.CREATE,
    OpenSearchOperationModes.INDEX,
    OpenSearchOperationModes.UPDATE
}

DEFAULT_OPENSEARCH_OPERATION_MODE = OpenSearchOperationModes.INDEX


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
    timeout: float = DEFAULT_OPENSEARCH_TIMEOUT
    update_index_settings: bool = False
    update_mappings: bool = False
    index_settings: Optional[dict] = None
    verify_certificates: bool = True
    operation_mode: str = DEFAULT_OPENSEARCH_OPERATION_MODE
    upsert: bool = False

    @staticmethod
    def from_dict(opensearch_target_config_dict: dict) -> 'OpenSearchTargetConfig':
        secrets = get_resolved_parameter_values_from_file_path_env_name(
            opensearch_target_config_dict['secrets']['parametersFromFile']
        )
        operation_mode = opensearch_target_config_dict.get(
            'operationMode', DEFAULT_OPENSEARCH_OPERATION_MODE
        )
        if operation_mode not in VALID_OPENSEARCH_OPERATION_MODES:
            raise ValueError(f'invalid operation mode: {operation_mode}')
        return OpenSearchTargetConfig(
            hostname=opensearch_target_config_dict['hostname'],
            port=opensearch_target_config_dict['port'],
            username=secrets['username'],
            password=secrets['password'],
            index_name=opensearch_target_config_dict['indexName'],
            timeout=opensearch_target_config_dict.get('timeout', DEFAULT_OPENSEARCH_TIMEOUT),
            update_index_settings=opensearch_target_config_dict.get('updateIndexSettings', False),
            update_mappings=opensearch_target_config_dict.get('updateMappings', False),
            index_settings=opensearch_target_config_dict.get('indexSettings'),
            verify_certificates=opensearch_target_config_dict.get('verifyCertificates', True),
            operation_mode=operation_mode,
            upsert=opensearch_target_config_dict.get('upsert', False)
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
                target_config_dict['openSearch']
            )
        )


@dataclass(frozen=True)
class BigQueryToOpenSearchInitialStateConfig:
    start_timestamp: datetime

    @staticmethod
    def from_dict(initial_state_config_dict: dict) -> 'BigQueryToOpenSearchInitialStateConfig':
        return BigQueryToOpenSearchInitialStateConfig(
            start_timestamp=datetime.fromisoformat(initial_state_config_dict['startTimestamp'])
        )


@dataclass(frozen=True)
class BigQueryToOpenSearchStateConfig:
    initial_state: BigQueryToOpenSearchInitialStateConfig
    state_file: StateFileConfig

    @staticmethod
    def from_dict(state_config_dict: dict) -> 'BigQueryToOpenSearchStateConfig':
        return BigQueryToOpenSearchStateConfig(
            initial_state=BigQueryToOpenSearchInitialStateConfig.from_dict(
                state_config_dict['initialState']
            ),
            state_file=StateFileConfig.from_dict(
                state_config_dict['stateFile']
            )
        )


DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True)
class BigQueryToOpenSearchConfig:
    source: BigQueryToOpenSearchSourceConfig
    field_names_for: BigQueryToOpenSearchFieldNamesForConfig
    target: BigQueryToOpenSearchTargetConfig
    state: BigQueryToOpenSearchStateConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'BigQueryToOpenSearchConfig':
        return BigQueryToOpenSearchConfig(
            source=BigQueryToOpenSearchSourceConfig.from_dict(item_config_dict['source']),
            field_names_for=BigQueryToOpenSearchFieldNamesForConfig.from_dict(
                item_config_dict['fieldNamesFor']
            ),
            target=BigQueryToOpenSearchTargetConfig.from_dict(item_config_dict['target']),
            state=BigQueryToOpenSearchStateConfig.from_dict(item_config_dict['state']),
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
