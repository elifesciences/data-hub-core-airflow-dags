from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict


DEP_ENV = 'test'


def get_data_config(
    conf_dict: WebApiConfigDict,
    dep_env: str = DEP_ENV
) -> WebApiConfig:
    return WebApiConfig.from_dict(
        conf_dict,
        deployment_env=dep_env
    )
