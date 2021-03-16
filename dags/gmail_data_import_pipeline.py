# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path

from google.cloud import bigquery

from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    load_file_into_bq,
)

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.gmail_data.get_gmail_data_config import (
    GmailGetDataConfig
)

from data_pipeline.utils.pipeline_file_io import (
    get_yaml_file_as_dict
)

from data_pipeline.gmail_data.get_gmail_data import (
    get_gmail_service_for_user_id,
    get_label_list,
    write_dataframe_to_csv_file
)

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
GMAIL_ACCOUNT_SECRET_FILE = 'DELETE/elife-data-pipeline-test.json'

GMAIL_DATA_USER_ID_ENV = "GMAIL_DATA_USER_ID"
GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME = "GMAIL_DATA_CONFIG_FILE_PATH"
DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"

DAG_ID = "Gmail_Data_Import_Pipeline"

LOGGER = logging.getLogger(__name__)


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_data_config(**kwargs):
    conf_file_path = get_env_var_or_use_default(
        GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME, ""
    )
    LOGGER.info('conf_file_path: %s', conf_file_path)
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    LOGGER.info('data_config_dict: %s', data_config_dict)
    kwargs["ti"].xcom_push(
        key="data_config_dict",
        value=data_config_dict
    )


def data_config_from_xcom(context):
    dag_context = context["ti"]
    LOGGER.info('dag_context: %s', dag_context)
    data_config_dict = dag_context.xcom_pull(
        key="data_config_dict", task_ids="get_data_config"
    )
    LOGGER.info('data_config_dict: %s', data_config_dict)
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV)
    data_config = GmailGetDataConfig(
        data_config_dict, deployment_env)
    LOGGER.info('data_config: %s', data_config)
    return data_config


def get_gmail_service():
    return get_gmail_service_for_user_id(
        GMAIL_ACCOUNT_SECRET_FILE,
        GMAIL_SCOPES,
        GMAIL_DATA_USER_ID_ENV
    )


def create_label_list_table(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    schema_json = download_s3_json_object(
        data_config.schema_file_s3_bucket_labels,
        data_config.schema_file_s3_object_labels
    )

    kwargs["ti"].xcom_push(
        key="data_schema", value=schema_json
    )

    create_table_if_not_exist(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset,
        table_name=data_config.table_name_labels,
        json_schema=schema_json
    )
    LOGGER.info('Created table: %s', data_config.table_name_labels)


def gmail_label_data_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)

    with TemporaryDirectory() as tmp_dir:
        filename = Path(tmp_dir)/data_config.table_name_labels

        write_dataframe_to_csv_file(
            get_label_list(
                get_gmail_service(),
                GMAIL_DATA_USER_ID_ENV
            ),
            filename
        )

        LOGGER.info('Created file: %s', filename)

        load_file_into_bq(
            filename=filename,
            dataset_name=data_config.dataset,
            table_name=data_config.table_name_labels,
            project_name=data_config.project_name,
            source_format=bigquery.SourceFormat.CSV
        )
        LOGGER.info('Loaded table: %s', data_config.table_name_labels)


GMAIL_DATA_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    dagrun_timeout=timedelta(days=1)
)

get_data_config_task = create_python_task(
    GMAIL_DATA_DAG,
    "get_data_config",
    get_data_config,
    retries=5
)

create_label_list_table_task = create_python_task(
    GMAIL_DATA_DAG,
    "create_label_list_table",
    create_label_list_table,
    retries=5
)

gmail_label_data_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_label_data_etl",
    gmail_label_data_etl,
    retries=5
)

# pylint: disable=pointless-statement
(
    get_data_config_task
    >> create_label_list_table_task
    >> gmail_label_data_etl_task
)
