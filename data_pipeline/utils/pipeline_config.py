import os
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence, Type, TypeVar, Union

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict, read_file_content
from data_pipeline.utils.pipeline_config_typing import (
    AirflowConfigDict,
    BigQueryIncludeExcludeSourceConfigDict,
    BigQuerySourceConfigDict,
    BigQueryTargetConfigDict,
    BigQueryWrappedExcludeSourceConfigDict,
    BigQueryWrappedSourceConfigDict,
    MappingConfigDict,
    ParameterFromFileConfigDict,
    StateFileConfigDict
)


LOGGER = logging.getLogger(__name__)

T = TypeVar('T')

ConfigDictT = TypeVar('ConfigDictT')


class ConfigKeys:
    DATA_PIPELINE_CONFIG_ID = 'dataPipelineId'


class PipelineEnvironmentVariables:
    DEPLOYMENT_ENV = 'DEPLOYMENT_ENV'


DEFAULT_DEPLOYMENT_ENV = 'ci'

DEFAULT_ENVIRONMENT_PLACEHOLDER = '{ENV}'

SECRET_VALUE_PLACEHOLDER = '***'


@dataclass(frozen=True)
class BigQuerySourceConfig:
    project_name: str
    sql_query: str
    ignore_not_found: bool = False

    @staticmethod
    def from_dict(source_config_dict: BigQuerySourceConfigDict) -> 'BigQuerySourceConfig':
        return BigQuerySourceConfig(
            project_name=source_config_dict['projectName'],
            sql_query=source_config_dict['sqlQuery'],
            ignore_not_found=source_config_dict.get('ignoreNotFound', False)
        )


@dataclass(frozen=True)
class BigQueryWrappedSourceConfig:
    bigquery: BigQuerySourceConfig

    @staticmethod
    def from_dict(
        source_config_dict: BigQueryWrappedSourceConfigDict
    ) -> 'BigQueryWrappedSourceConfig':
        return BigQueryWrappedSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            )
        )


@dataclass(frozen=True)
class BigQueryWrappedExcludeSourceConfig:
    bigquery: BigQuerySourceConfig
    key_field_name_from_include: str

    @staticmethod
    def from_dict(
        source_config_dict: BigQueryWrappedExcludeSourceConfigDict
    ) -> 'BigQueryWrappedExcludeSourceConfig':
        return BigQueryWrappedExcludeSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            ),
            key_field_name_from_include=source_config_dict['keyFieldNameFromInclude']
        )

    @staticmethod
    def from_optional_dict(
        exclude_config_dict: Optional[BigQueryWrappedExcludeSourceConfigDict]
    ) -> Optional['BigQueryWrappedExcludeSourceConfig']:
        if exclude_config_dict is None:
            return None
        return BigQueryWrappedExcludeSourceConfig.from_dict(exclude_config_dict)


@dataclass(frozen=True)
class BigQueryIncludeExcludeSourceConfig:
    include: BigQueryWrappedSourceConfig
    exclude:  Optional[BigQueryWrappedExcludeSourceConfig] = None

    @staticmethod
    def from_dict(
        include_exclude_config_dict: BigQueryIncludeExcludeSourceConfigDict
    ) -> 'BigQueryIncludeExcludeSourceConfig':
        LOGGER.debug('include_exclude_config_dict: %r', include_exclude_config_dict)
        return BigQueryIncludeExcludeSourceConfig(
            include=BigQueryWrappedSourceConfig.from_dict(
                include_exclude_config_dict['include']
            ),
            exclude=BigQueryWrappedExcludeSourceConfig.from_optional_dict(
                include_exclude_config_dict.get('exclude')
            )
        )

    @staticmethod
    def from_optional_dict(
        include_exclude_config_dict: Optional[BigQueryIncludeExcludeSourceConfigDict]
    ) -> Optional['BigQueryIncludeExcludeSourceConfig']:
        if include_exclude_config_dict is None:
            return None
        return BigQueryIncludeExcludeSourceConfig.from_dict(include_exclude_config_dict)


@dataclass(frozen=True)
class BigQueryTargetConfig:
    project_name: str
    dataset_name: str
    table_name: str

    @staticmethod
    def from_dict(target_config_dict: BigQueryTargetConfigDict) -> 'BigQueryTargetConfig':
        return BigQueryTargetConfig(
            project_name=target_config_dict['projectName'],
            dataset_name=target_config_dict['datasetName'],
            table_name=target_config_dict['tableName']
        )


@dataclass(frozen=True)
class StateFileConfig:
    bucket_name: str
    object_name: str

    @staticmethod
    def from_dict(state_file_config_dict: StateFileConfigDict) -> 'StateFileConfig':
        return StateFileConfig(
            bucket_name=state_file_config_dict['bucketName'],
            object_name=state_file_config_dict['objectName']
        )


@dataclass(frozen=True)
class AirflowConfig:
    dag_parameters: dict
    task_parameters: dict

    @staticmethod
    def from_dict(
        airflow_config_dict: AirflowConfigDict,
        default_airflow_config: Optional['AirflowConfig'] = None
    ) -> 'AirflowConfig':
        return AirflowConfig(
            dag_parameters={
                **(default_airflow_config.dag_parameters if default_airflow_config else {}),
                **(airflow_config_dict.get('dagParameters') or {})
            },
            task_parameters={
                **(default_airflow_config.task_parameters if default_airflow_config else {}),
                **(airflow_config_dict.get('taskParameters') or {})
            }
        )

    @staticmethod
    def from_optional_dict(
        airflow_config_dict: Optional[AirflowConfigDict],
        default_airflow_config: Optional['AirflowConfig'] = None
    ) -> 'AirflowConfig':
        return AirflowConfig.from_dict(
            airflow_config_dict or {},
            default_airflow_config=default_airflow_config
        )


def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str = DEFAULT_ENVIRONMENT_PLACEHOLDER
):
    new_dict = {}
    for key, val in original_dict.items():
        if isinstance(val, dict):
            tmp = update_deployment_env_placeholder(
                val,
                deployment_env,
                environment_placeholder
            )
            new_dict[key] = tmp
        elif isinstance(val, list):
            updated_val = []
            for elem in val:
                if isinstance(elem, dict):
                    updated_val.append(
                        update_deployment_env_placeholder(
                            elem,
                            deployment_env,
                            environment_placeholder
                        )
                    )
                else:
                    updated_val.append(
                        replace_env_placeholder(
                            elem,
                            deployment_env,
                            environment_placeholder
                        )
                    )
            new_dict[key] = updated_val
        else:
            new_dict[key] = replace_env_placeholder(
                original_dict[key],
                deployment_env,
                environment_placeholder
            )
    return new_dict


def replace_env_placeholder(
        param_value,
        deployment_env: str,
        environment_placeholder: str
):
    new_value = param_value
    if isinstance(param_value, str):
        new_value = param_value.replace(
            environment_placeholder,
            deployment_env
        )
    return new_value


TRUE_VALUES = {'true', '1', 't', 'y', 'yes'}
FALSE_VALUES = {'false', '0', 'f', 'n', 'no'}


def str_to_bool(value: str, default_value=None) -> bool:
    if not value and default_value is not None:
        return default_value
    lower_value = value.lower()
    if lower_value in TRUE_VALUES:
        return True
    if lower_value in FALSE_VALUES:
        return False
    raise ValueError(f'unrecognised boolean value: {value}')


def get_environment_variable_value(
        key: str,
        value_converter: Optional[Type] = None,
        required: bool = False,
        default_value: Optional[str] = None):
    value = os.getenv(key)
    if not value:
        if required:
            raise KeyError(f'environment variable {key} ({value_converter}) required')
        return default_value
    if value_converter:
        value = value_converter(value)
    return value


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_deployment_env() -> str:
    return get_env_var_or_use_default(
        PipelineEnvironmentVariables.DEPLOYMENT_ENV,
        DEFAULT_DEPLOYMENT_ENV
    )


def get_resolved_parameter_values_from_file_path_env_name(
    parameters_from_file: Sequence[ParameterFromFileConfigDict]
) -> dict:
    params = {
        param["parameterName"]: read_file_content(os.environ[param["filePathEnvName"]])
        for param in parameters_from_file
        if os.getenv(param["filePathEnvName"])
    }
    return params


def get_pipeline_config_for_env_name_and_config_parser(
    config_file_path_env_name: str,
    config_parser_fn: Callable[[ConfigDictT], T]
) -> T:
    deployment_env = get_deployment_env()
    LOGGER.info('deployment_env: %s', deployment_env)
    conf_file_path = os.environ[config_file_path_env_name]
    pipeline_config_dict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(conf_file_path),
        deployment_env=deployment_env
    )
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    pipeline_config = config_parser_fn(pipeline_config_dict)
    LOGGER.info('pipeline_config: %s', pipeline_config)
    return pipeline_config


@dataclass(frozen=True)
class MappingConfig:
    mapping: Mapping[str, Any] = field(repr=False)
    printable_mapping: Mapping[str, Any]

    @staticmethod
    def from_dict(mapping_config_dict: MappingConfigDict) -> 'MappingConfig':
        mapping = dict(mapping_config_dict)
        secrets_config_list = mapping.pop('parametersFromFile', [])
        LOGGER.debug('secrets_config_list: %r', secrets_config_list)
        secrets_dict = get_resolved_parameter_values_from_file_path_env_name(
            secrets_config_list
        )
        LOGGER.debug('secrets.keys: %r', secrets_dict.keys())
        mapping.update(secrets_dict)
        secret_keys = secrets_dict.keys()
        printable_mapping = {
            key: SECRET_VALUE_PLACEHOLDER if key in secret_keys else value
            for key, value in mapping.items()
        }
        return MappingConfig(
            mapping=mapping,
            printable_mapping=printable_mapping
        )


def parse_key_path(key_path: Optional[Union[str, Sequence[str]]]) -> Sequence[str]:
    if isinstance(key_path, list):
        return key_path
    if isinstance(key_path, str):
        return key_path.split('.')
    if key_path is not None:
        raise TypeError(f'unsupported type for key path: {type(key_path)}')
    return []


def parse_required_non_empty_key_path(
    key_path: Optional[Union[str, Sequence[str]]]
) -> Sequence[str]:
    parsed_key_path = parse_key_path(key_path)
    if not parsed_key_path:
        raise ValueError('key path required')
    return parsed_key_path
