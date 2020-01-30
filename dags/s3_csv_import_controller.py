import os

from airflow.models import DAG

from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_yaml_file_as_dict
)
from data_pipeline.s3_csv_data.s3_csv_config import MultiS3CsvConfig
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    simple_trigger_dag,
    get_default_args,
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


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


# pylint: disable=unused-argument
def trigger_dag(**context):
    conf_file_path = get_env_var_or_use_default(
        S3_CSV_CONFIG_FILE_PATH_ENV_NAME, ""
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    data_config = MultiS3CsvConfig(data_config_dict,)
    for s3_csv_config in data_config.s3_csv_config:
        simple_trigger_dag(dag_id=TARGET_DAG, conf=s3_csv_config)


S3_CSV_CONTROLLER_DAG = DAG(
    dag_id="S3_CSV_Import_Pipeline_Controller",
    schedule_interval=get_env_var_or_use_default(
        S3_CSV_SCHEDULE_INTERVAL_ENV_NAME
    ),
    default_args=get_default_args(),

)

TRIGGER_S3_CSV_ETL_DAG_TASK = create_python_task(
    S3_CSV_CONTROLLER_DAG, "trigger_s3_csv_etl_dag",
    trigger_dag,
)
