import os
from typing import Sequence, Tuple, cast

from data_pipeline.utils.pipeline_file_io import read_file_content

from data_pipeline.generic_web_api.generic_web_api_config_typing import (
    WebApiAuthenticationValueConfigDict
)


class WebApiAuthentication:
    def __init__(
            self,
            auth_type: str,
            auth_param_val_list: Sequence[WebApiAuthenticationValueConfigDict],
    ):
        self.authentication_type = auth_type.lower()
        if auth_type == 'basic':
            assert len(auth_param_val_list) == 2
        self.auth_val_list = cast(
            Tuple[str, str],
            [
                get_auth_param_value(auth_val_conf)
                for auth_val_conf in auth_param_val_list
            ]
        ) if auth_type == 'basic' else None


def get_auth_param_value(
    auth_val_conf: WebApiAuthenticationValueConfigDict
) -> str:
    val = auth_val_conf.get("value", None)
    if not val:
        env_var_key_with_val = auth_val_conf.get("envVariableHoldingAuthValue")
        val = (
            os.getenv(env_var_key_with_val) if env_var_key_with_val else None
        )
    if not val:
        val = read_file_content(
            os.environ[auth_val_conf["envVariableContainingPathToAuthFile"]]
        )
    assert val is not None
    return val
