import os

from data_pipeline.utils.pipeline_file_io import read_file_content


class WebApiAuthentication:
    def __init__(
            self,
            auth_type: str,
            auth_param_val_list: list = None,
    ):
        self.authentication_type = auth_type.lower()

        self.auth_val_list = [
            get_auth_param_value(auth_val_conf)
            for auth_val_conf in auth_param_val_list
        ] if auth_type == 'basic' else None


def get_auth_param_value(auth_val_conf: dict):
    val = (
        auth_val_conf.get("value", None)
        or
        os.getenv(
            auth_val_conf.get(
                "envVariableHoldingAuthValue", ""
            ),
            None
        )
        or
        (
            read_file_content(
                os.getenv(
                    auth_val_conf.get(
                        "envVariableContainingPathToAuthFile"
                    )
                )
            )
        )
    )
    return val
