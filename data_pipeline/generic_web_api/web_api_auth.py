import os
from typing import Tuple, cast

from data_pipeline.utils.pipeline_file_io import read_file_content


class WebApiAuthentication:
    def __init__(
            self,
            auth_type: str,
            auth_param_val_list: list,
    ):
        self.authentication_type = auth_type.lower()
        if auth_type == 'basic':
            assert len(auth_param_val_list) == 2
        self.auth_val_list = cast([
            get_auth_param_value(auth_val_conf)
            for auth_val_conf in auth_param_val_list
        ], Tuple[str, str]) if auth_type == 'basic' else None


def get_auth_param_value(auth_val_conf: dict):
    val = auth_val_conf.get("value", None)
    if not val:
        env_var_key_with_val = auth_val_conf.get("envVariableHoldingAuthValue")
        val = (
            os.getenv(env_var_key_with_val) if env_var_key_with_val else None
        )
    if not val:
        val = read_file_content(
            os.getenv(
                auth_val_conf.get(
                    "envVariableContainingPathToAuthFile"
                )
            )
        )

    return val
