# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import functools
import os
import logging
from datetime import timedelta

from data_pipeline.generic_web_api.generic_web_api_config import (
    MultiWebApiConfig,
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    generic_web_api_data_etl
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task,
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"


WEB_API_CONFIG_FILE_PATH_ENV_NAME = (
    "WEB_API_CONFIG_FILE_PATH"
)


def get_multi_web_api_config() -> MultiWebApiConfig:
    conf_file_path = os.getenv(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    return MultiWebApiConfig(data_config_dict)


def web_api_data_etl(config_id: str, **_kwargs):
    multi_web_api_config = get_multi_web_api_config()
    data_config_dict = multi_web_api_config.web_api_config[config_id]
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )

    data_config = WebApiConfig.from_dict(data_config_dict, deployment_env=dep_env)
    generic_web_api_data_etl(
        data_config=data_config,
    )


def get_dag_id_for_web_api_config_dict(web_api_config_dict: WebApiConfigDict) -> str:
    return f'Web_API.{web_api_config_dict["dataPipelineId"]}'


def create_web_api_dags():
    multi_web_api_config = get_multi_web_api_config()
    for config_id, web_api_config_dict in multi_web_api_config.web_api_config.items():
        with create_dag(
            dag_id=get_dag_id_for_web_api_config_dict(web_api_config_dict),
            description=web_api_config_dict.get('description'),
            schedule=None,
            dagrun_timeout=timedelta(days=1)
        ) as dag:
            create_python_task(
                dag=dag,
                task_id="web_api_data_etl",
                python_callable=functools.partial(
                    web_api_data_etl,
                    config_id=config_id
                )
            )


create_web_api_dags()
