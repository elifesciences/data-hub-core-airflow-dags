import os
import logging
from datetime import timedelta
from airflow import DAG

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet
)
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    etl_google_spreadsheet
)

LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = None


DAG_ID = "Google_Spreadsheet_Data_Pipeline"
G_SPREADSHEET_DAG = DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)


def get_env_var_or_use_default(env_var_name, default_value):
    return os.getenv(env_var_name, default_value)


def google_spreadsheet_data_etl(**kwargs):
    data_config_dict = kwargs["dag_run"].conf
    data_config = MultiCsvSheet(data_config_dict)
    dep_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    env_based_data_config = (
        data_config if dep_env is None
        else data_config.set_dataset_name(dep_env)
    )
    etl_google_spreadsheet(env_based_data_config)


G_SPREADSHEET_ETL_TASK = create_python_task(
    G_SPREADSHEET_DAG, "google_spreadsheet_data_etl",
    google_spreadsheet_data_etl,
)
