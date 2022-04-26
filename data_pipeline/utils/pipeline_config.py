import os
import logging
from typing import Callable, T, NamedTuple, Sequence

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict, read_file_content


LOGGER = logging.getLogger(__name__)


class ConfigKeys:
    DATA_PIPELINE_CONFIG_ID = 'dataPipelineId'


class PipelineEnvironmentVariables:
    DEPLOYMENT_ENV = 'DEPLOYMENT_ENV'


DEFAULT_DEPLOYMENT_ENV = 'ci'

DEFAULT_ENVIRONMENT_PLACEHOLDER = '{ENV}'


class BigQuerySourceConfig(NamedTuple):
    project_name: str
    sql_query: str
    ignore_not_found: bool = False

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'BigQuerySourceConfig':
        return BigQuerySourceConfig(
            project_name=source_config_dict['projectName'],
            sql_query=source_config_dict['sqlQuery'],
            ignore_not_found=source_config_dict.get('ignoreNotFound', False)
        )


class BigQueryTargetConfig(NamedTuple):
    project_name: str
    dataset_name: str
    table_name: str

    @staticmethod
    def from_dict(target_config_dict: dict) -> 'BigQueryTargetConfig':
        return BigQueryTargetConfig(
            project_name=target_config_dict['projectName'],
            dataset_name=target_config_dict['datasetName'],
            table_name=target_config_dict['tableName']
        )


class StateFileConfig(NamedTuple):
    bucket_name: str
    object_name: str

    @staticmethod
    def from_dict(state_file_config_dict: dict) -> 'StateFileConfig':
        return StateFileConfig(
            bucket_name=state_file_config_dict['bucketName'],
            object_name=state_file_config_dict['objectName']
        )


def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str = DEFAULT_ENVIRONMENT_PLACEHOLDER
):
    new_dict = dict()
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
    raise ValueError('unrecognised boolean value: %r' % value)


def get_environment_variable_value(
        key: str,
        value_converter: Callable[[str], T] = None,
        required: bool = False,
        default_value: T = None) -> T:
    value = os.getenv(key)
    if not value:
        if required:
            raise KeyError('environment variable %s (%s) required' % (
                key, value_converter
            ))
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
    parameters_from_file: Sequence[dict]
) -> dict:
    params = {
        param.get("parameterName"):
            read_file_content(
                os.getenv(
                    param.get("filePathEnvName")
                )
            )
        for param in parameters_from_file
        if os.getenv(param.get("filePathEnvName"))
    }
    return params


def get_pipeline_config_for_env_name_and_config_parser(
    config_file_path_env_name: str,
    config_parser_fn: Callable[[dict], T]
) -> T:
    deployment_env = get_deployment_env()
    LOGGER.info('deployment_env: %s', deployment_env)
    conf_file_path = os.getenv(
        config_file_path_env_name
    )
    pipeline_config_dict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(conf_file_path),
        deployment_env=deployment_env
    )
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    pipeline_config = config_parser_fn(pipeline_config_dict)
    LOGGER.info('pipeline_config: %s', pipeline_config)
    return pipeline_config
