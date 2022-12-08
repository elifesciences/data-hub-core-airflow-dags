# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    generic_web_api_data_etl
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task,
)


LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

DAG_ID = "Generic_Web_Api_Data_Pipeline"

GENERIC_WEB_API_DATA = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    dagrun_timeout=timedelta(days=1)
)


def web_api_data_etl(**kwargs):
    data_config_dict = kwargs["dag_run"].conf
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )

    data_config = WebApiConfig(data_config_dict, deployment_env=dep_env)
    generic_web_api_data_etl(
        data_config=data_config,
    )


GENERIC_WEB_API_DATA_ETL_TASK = create_python_task(
    GENERIC_WEB_API_DATA, "web_api_data_etl",
    web_api_data_etl,
)
