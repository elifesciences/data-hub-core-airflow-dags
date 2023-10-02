# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet
)
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    etl_google_spreadsheet
)

LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

DAG_ID = "Google_Spreadsheet_Data_Pipeline"

G_SPREADSHEET_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=None,
    dagrun_timeout=timedelta(minutes=60)
)


def google_spreadsheet_data_etl(**kwargs):
    data_config_dict = kwargs["dag_run"].conf
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    data_config = MultiCsvSheet(data_config_dict, dep_env)
    etl_google_spreadsheet(data_config)


G_SPREADSHEET_ETL_TASK = create_python_task(
    G_SPREADSHEET_DAG, "google_spreadsheet_data_etl",
    google_spreadsheet_data_etl,
)
