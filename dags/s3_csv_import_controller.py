# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict

from data_pipeline.s3_csv_data.s3_csv_config import MultiS3CsvConfig
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    trigger_data_pipeline_dag,
    create_dag,
    create_python_task
)

S3_CSV_SCHEDULE_INTERVAL_ENV_NAME = (
    "S3_CSV_SCHEDULE_INTERVAL"
)
S3_CSV_CONFIG_FILE_PATH_ENV_NAME = (
    "S3_CSV_CONFIG_FILE_PATH"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

TARGET_DAG = "S3_CSV_Data_Pipeline"
DAG_ID = "S3_CSV_Import_Pipeline_Controller"


# pylint: disable=unused-argument
def trigger_s3_csv_import_pipeline_dag(**context):
    conf_file_path = os.getenv(
        S3_CSV_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    data_config = MultiS3CsvConfig(data_config_dict,)
    for s3_csv_config in data_config.s3_csv_config:
        trigger_data_pipeline_dag(
            dag_id=TARGET_DAG, conf=s3_csv_config
        )


S3_CSV_CONTROLLER_DAG = create_dag(
    dag_id="S3_CSV_Import_Pipeline_Controller",
    schedule_interval=os.getenv(
        S3_CSV_SCHEDULE_INTERVAL_ENV_NAME
    )
)

TRIGGER_S3_CSV_ETL_DAG_TASK = create_python_task(
    S3_CSV_CONTROLLER_DAG, "trigger_s3_csv_etl_dag",
    trigger_s3_csv_import_pipeline_dag,
)
