# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.generic_web_api.generic_web_api_config import (
    MultiWebApiConfig
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    simple_trigger_dag,
    create_dag,
    create_python_task,
    get_suffix_for_config
)

WEB_API_SCHEDULE_INTERVAL_ENV_NAME = (
    "WEB_API_SCHEDULE_INTERVAL"
)
WEB_API_CONFIG_FILE_PATH_ENV_NAME = (
    "WEB_API_CONFIG_FILE_PATH"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

TARGET_DAG_ID = "Generic_Web_Api_Data_Pipeline"
DAG_ID = "Web_Api_Data_Import_Pipeline_Controller"


# pylint: disable=unused-argument
def trigger_web_api_data_import_pipeline_dag(**context):
    conf_file_path = os.getenv(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    data_config = MultiWebApiConfig(data_config_dict,)
    for web_api_config in data_config.web_api_config.values():
        simple_trigger_dag(
            dag_id=TARGET_DAG_ID,
            conf=web_api_config,
            suffix=get_suffix_for_config(web_api_config)
        )


WEB_API_CONTROLLER_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        WEB_API_SCHEDULE_INTERVAL_ENV_NAME
    )
)

TRIGGER_S3_CSV_ETL_DAG_TASK = create_python_task(
    WEB_API_CONTROLLER_DAG, "trigger_web_api_etl_dag",
    trigger_web_api_data_import_pipeline_dag,
)
