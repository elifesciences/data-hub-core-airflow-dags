# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
from dags.gmail_data_import_pipeline import LOGGER
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    trigger_data_pipeline_dag,
    create_dag,
    create_python_task
)
from data_pipeline.gmail_data.get_gmail_data_config import MultiGmailDataConfig

GMAIL_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = "GMAIL_DATA_PIPELINE_SCHEDULE_INTERVAL"
GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME = "GMAIL_DATA_CONFIG_FILE_PATH"

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

TARGET_DAG_ID = "Gmail_Data_Import_Pipeline"
DAG_ID = "Gmail_Data_Import_Pipeline_Controller"


# pylint: disable=unused-argument
def trigger_gmail_data_import_pipeline_dag(**context):
    conf_file_path = os.getenv(
        GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    LOGGER.info("data_config_dict: %s", data_config_dict)
    data_config = MultiGmailDataConfig(data_config_dict)
    LOGGER.info("data_config: %s", data_config)
    for gmail_data_config in data_config.gmail_data_config.values():
        trigger_data_pipeline_dag(
            dag_id=TARGET_DAG_ID,
            conf=gmail_data_config
        )


GMAIL_CONTROLLER_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        GMAIL_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME
    )
)

TRIGGER_GMAIL_ETL_DAG_TASK = create_python_task(
    GMAIL_CONTROLLER_DAG,
    "trigger_gmail_etl_dag",
    trigger_gmail_data_import_pipeline_dag,
)
