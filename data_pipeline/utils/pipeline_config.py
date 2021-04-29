import os
from typing import Callable, T


class ConfigKeys:
    DATA_PIPELINE_CONFIG_ID = 'dataPipelineId'


def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str,
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
