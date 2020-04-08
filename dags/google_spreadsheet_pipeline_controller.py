import os

from airflow import DAG

from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiSpreadsheetConfig,
)
from data_pipeline import get_yaml_file_as_dict
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    simple_trigger_dag,
    create_python_task
)

GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL_ENV_NAME = (
    "GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL"
)
SPREADSHEET_CONFIG_FILE_PATH_ENV_NAME = (
    "SPREADSHEET_CONFIG_FILE_PATH"
)

TARGET_DAG_ID = "Google_Spreadsheet_Data_Pipeline"
DAG_ID = 'Google_Spreadsheet_Import_Pipeline_Controller'


# pylint: disable=unused-argument
def trigger_spreadsheet_data_pipeline_dag(**kwargs):
    conf_file_path = os.getenv(
        SPREADSHEET_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)

    data_config = MultiSpreadsheetConfig(data_config_dict,)
    for spreadsheet_config in data_config.spreadsheets_config.values():
        simple_trigger_dag(dag_id=TARGET_DAG_ID, conf=spreadsheet_config)


SPREADSHEET_CONTROLLER_DAG = DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    schedule_interval=os.getenv(
        GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL_ENV_NAME
    ),
)

TRIGGER_SPREADSHEET_ETL_DAG_TASK = create_python_task(
    SPREADSHEET_CONTROLLER_DAG,
    "trigger_google_spreadsheet_etl_dag",
    trigger_spreadsheet_data_pipeline_dag,
)
