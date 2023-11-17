# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from data_pipeline.generic_web_api.generic_web_api_config import (
    MultiWebApiConfig,
    WebApiConfig
)
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

DAG_ID = "Generic_Web_Api_Data_Pipeline"


def web_api_data_etl(**kwargs):
    data_config_dict = kwargs["dag_run"].conf
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )

    data_config = WebApiConfig.from_dict(data_config_dict, deployment_env=dep_env)
    generic_web_api_data_etl(
        data_config=data_config,
    )


WEB_API_CONFIG_FILE_PATH_ENV_NAME = (
    "WEB_API_CONFIG_FILE_PATH"
)


def get_multi_web_api_config() -> MultiWebApiConfig:
    conf_file_path = os.getenv(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    return MultiWebApiConfig(data_config_dict)


MULTI_WEB_API_CONFIG = get_multi_web_api_config()

for web_api_config_dict in MULTI_WEB_API_CONFIG.web_api_config.values():
    GENERIC_WEB_API_DATA = create_dag(
        dag_id=f'Web_API.{web_api_config_dict["dataPipelineId"]}',
        description=web_api_config_dict.get('description'),
        schedule=None,
        dagrun_timeout=timedelta(days=1)
    )

    GENERIC_WEB_API_DATA_ETL_TASK = create_python_task(
        GENERIC_WEB_API_DATA, "web_api_data_etl",
        web_api_data_etl,
    )
