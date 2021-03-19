# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory

from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    load_file_into_bq,
    generate_schema_from_file
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
    write_dataframe_to_jsonl_file,
    get_link_message_thread_ids
)

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
GMAIL_ACCOUNT_SECRET_FILE_ENV = 'GOOGLE_APPLICATION_CREDENTIALS'

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
    data_config_dict = dag_context.xcom_pull(
        key="data_config_dict", task_ids="get_data_config"
    )
    LOGGER.info('data_config_dict: %s', data_config_dict)
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV)
    data_config = GmailGetDataConfig(
        data_config_dict, deployment_env)
    return data_config


def get_gmail_user_id():
    return get_env_var_or_use_default(GMAIL_DATA_USER_ID_ENV)


def get_gmail_service():
    secret_file = get_env_var_or_use_default(GMAIL_ACCOUNT_SECRET_FILE_ENV, "")
    user_id = get_gmail_user_id()
    return get_gmail_service_for_user_id(
        secret_file=secret_file,
        scopes=GMAIL_SCOPES,
        user_id=user_id
    )


def gmail_label_data_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    table_name = data_config.table_name_labels

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, data_config.stage_file_name_labels)

        write_dataframe_to_jsonl_file(
            df_data_to_write=get_label_list(get_gmail_service(),  user_id),
            target_file_path=filename
        )

        LOGGER.info('Created file: %s', filename)

        generated_schema = generate_schema_from_file(filename)
        LOGGER.info('generated_schema: %s', generated_schema)

        create_table_if_not_exist(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset,
            table_name=table_name,
            json_schema=generated_schema
        )
        LOGGER.info('Created table: %s', table_name)

        load_file_into_bq(
            filename=filename,
            dataset_name=data_config.dataset,
            table_name=table_name,
            project_name=data_config.project_name,
            auto_detect_schema=True
        )
        LOGGER.info('Loaded table: %s', table_name)


def gmail_link_message_thread_ids_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    table_name = data_config.table_name_link_ids

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, data_config.stage_file_name_link_ids)

        write_dataframe_to_jsonl_file(
            df_data_to_write=get_link_message_thread_ids(get_gmail_service(),  user_id),
            target_file_path=filename
        )

        LOGGER.info('Created file: %s', filename)

        generated_schema = generate_schema_from_file(filename)
        LOGGER.info('generated_schema: %s', generated_schema)

        create_table_if_not_exist(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset,
            table_name=table_name,
            json_schema=generated_schema
        )
        LOGGER.info('Created table: %s', table_name)

        load_file_into_bq(
            filename=filename,
            dataset_name=data_config.dataset,
            table_name=table_name,
            project_name=data_config.project_name,
            auto_detect_schema=True
        )
        LOGGER.info('Loaded table: %s', table_name)


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

gmail_label_data_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_label_data_etl",
    gmail_label_data_etl,
    retries=5
)

gmail_link_message_thread_ids_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_link_message_thread_ids_etl",
    gmail_link_message_thread_ids_etl,
    retries=5
)

# pylint: disable=pointless-statement
(
    get_data_config_task
    >> [gmail_label_data_etl_task, gmail_link_message_thread_ids_etl_task]
)
