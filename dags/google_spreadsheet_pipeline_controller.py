import os
from airflow import DAG
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiSpreadsheetConfig,
)


from data_pipeline.utils.dags.data_pipeline_dag_utils import get_default_args, simple_trigger_dag, create_python_task

GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL_KEY = (
    "GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL_KEY"
)
GOOGLE_SPREADSHEET_CONFIG_S3_BUCKET_NAME = (
    "GOOGLE_SPREADSHEET_CONFIG_S3_BUCKET"
)
DEFAULT_GOOGLE_SPREADSHEET_CONFIG_S3_BUCKET_VALUE = "ci-elife-data-pipeline"
GOOGLE_SPREADSHEET_CONFIG_S3_OBJECT_KEY_NAME = (
    "GOOGLE_SPREADSHEET_CONFIG_S3_OBJECT_KEY"
)
DEFAULT_GOOGLE_SPREADSHEET_CONFIG_S3_OBJECT_KEY_VALUE = (
    "airflow_test/spreadsheet_data/spreadsheet-data-pipeline.config.yaml"
)

DEPLOYMENT_ENV = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = None

TARGET_DAG = "Google_Spreadsheet_Data_Pipeline"


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_data_config(**kwargs):

    data_config_dict = download_s3_yaml_object_as_json(
        get_env_var_or_use_default(
            GOOGLE_SPREADSHEET_CONFIG_S3_BUCKET_NAME,
            DEFAULT_GOOGLE_SPREADSHEET_CONFIG_S3_BUCKET_VALUE,
        ),
        get_env_var_or_use_default(
            GOOGLE_SPREADSHEET_CONFIG_S3_OBJECT_KEY_NAME,
            DEFAULT_GOOGLE_SPREADSHEET_CONFIG_S3_OBJECT_KEY_VALUE,
        ),
    )
    dep_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    data_config = MultiSpreadsheetConfig(data_config_dict, dep_env)
    for _, spreadsheet_config in data_config.spreadsheets_config.items():
        simple_trigger_dag(dag_id=TARGET_DAG, conf=spreadsheet_config)


SPREADSHEET_CONTROLLER_DAG = DAG(
    dag_id="Google_Spreadsheet_Import_Pipeline_Controller",
    default_args=get_default_args(),
    schedule_interval=get_env_var_or_use_default(
        GOOGLE_SPREADSHEET_SCHEDULE_INTERVAL_KEY
    ),
)

TRIGGER_SPEADSHEET_ETL_DAG_TASK = create_python_task(
    SPREADSHEET_CONTROLLER_DAG, "trigger_google_spreadsheet_etl_dag",
    get_data_config,
)
