# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory
import pandas as pd

from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
    generate_schema_from_file,
    create_or_extend_table_schema,
    delete_table_from_bq,
    load_from_temp_table_to_actual_table,
    get_distinct_values_from_bq,
    get_max_value_from_bq_table,
    does_bigquery_table_exist,
    copy_bq_table
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
    get_link_message_thread_ids,
    get_gmail_history_details,
    get_one_thread,
    dataframe_chunk
)

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
GMAIL_ACCOUNT_SECRET_FILE_ENV = 'GOOGLE_APPLICATION_CREDENTIALS'

GMAIL_DATA_USER_ID_ENV = "GMAIL_DATA_USER_ID"
GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME = "GMAIL_DATA_CONFIG_FILE_PATH"
DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"

DAG_ID = "Gmail_Data_Import_Pipeline"

CHUNK_SIZE = 3000

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
    LOGGER.info('data_config: %r', data_config)
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


def gmail_label_data_to_temp_table_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_labels

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, data_config.temp_file_name_labels)

        write_dataframe_to_jsonl_file(
            df_data_to_write=get_label_list(get_gmail_service(),  user_id),
            target_file_path=filename
        )

        LOGGER.info('Created file: %s', filename)

        generated_schema = generate_schema_from_file(filename)
        LOGGER.info('generated_schema: %s', generated_schema)

        create_or_extend_table_schema(
            gcp_project=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            full_file_location=filename,
            quoted_values_are_strings=True
        )

        load_file_into_bq(
            filename=filename,
            dataset_name=dataset_name,
            table_name=table_name,
            project_name=project_name
        )
        LOGGER.info('Loaded table: %s', table_name)


def gmail_thread_ids_list_to_temp_table_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_thread_ids

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, data_config.temp_file_name_thread_ids)

        write_dataframe_to_jsonl_file(
            df_data_to_write=get_link_message_thread_ids(get_gmail_service(),  user_id),
            target_file_path=filename
        )

        LOGGER.info('Created file: %s', filename)

        create_or_extend_table_schema(
            gcp_project=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            full_file_location=filename,
            quoted_values_are_strings=True
        )

        load_file_into_bq(
            filename=filename,
            dataset_name=dataset_name,
            table_name=table_name,
            project_name=project_name
        )
        LOGGER.info('Loaded table: %s', table_name)


def gmail_history_details_to_temp_table_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_history_details

    start_id = get_max_value_from_bq_table(
                    project_name=data_config.project_name,
                    dataset_name=dataset_name,
                    column_name=data_config.column_name_history_check,
                    table_name=data_config.table_name_thread_details
                )

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, data_config.temp_table_name_history_details)

        write_dataframe_to_jsonl_file(
            df_data_to_write=get_gmail_history_details(
                get_gmail_service(),
                user_id,
                str(start_id)
            ),
            target_file_path=filename
        )

        LOGGER.info('Created file: %s', filename)

        create_or_extend_table_schema(
            gcp_project=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            full_file_location=filename,
            quoted_values_are_strings=True
        )

        load_file_into_bq(
            filename=filename,
            dataset_name=dataset_name,
            table_name=table_name,
            project_name=project_name
        )
        LOGGER.info('Loaded table: %s', table_name)


def gmail_thread_details_from_temp_thread_ids_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.table_name_thread_details

    if does_bigquery_table_exist(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name
        ):
        df_thread_id_list = get_distinct_values_from_bq(
                            project_name=data_config.project_name,
                            dataset_name=dataset_name,
                            column_name=data_config.column_name_input,
                            table_name_source=data_config.temp_table_name_thread_ids
                        )
    else:
        df_thread_id_list = get_distinct_values_from_bq(
                            project_name=data_config.project_name,
                            dataset_name=dataset_name,
                            column_name=data_config.column_name_input,
                            table_name_source=data_config.temp_table_name_thread_ids,
                            table_name_for_exclusion=table_name
                        )

    # because of big amount of data created chunks of dataframe to load data
    for df_ids_part in dataframe_chunk(df_thread_id_list, CHUNK_SIZE):
        LOGGER.info('Last record of the df chunk: %s', df_ids_part.tail(1))
        df_thread_details = pd.concat([
                                        get_one_thread(get_gmail_service(), user_id, id)
                                        for id in df_ids_part[0]
                            ], ignore_index=True)
        with TemporaryDirectory() as tmp_dir:
            filename = os.path.join(tmp_dir, data_config.temp_file_name_thread_details)
            write_dataframe_to_jsonl_file(
                df_data_to_write=df_thread_details,
                target_file_path=filename
            )
            LOGGER.info('Created file: %s', filename)
            create_or_extend_table_schema(
                gcp_project=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                full_file_location=filename,
                quoted_values_are_strings=True
            )
            load_file_into_bq(
                filename=filename,
                dataset_name=dataset_name,
                table_name=table_name,
                project_name=project_name
            )
            LOGGER.info('Loaded table: %s', table_name)


def gmail_thread_details_from_temp_history_details_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    user_id = get_gmail_user_id()
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.table_name_thread_details

    df_thread_id_list = get_distinct_values_from_bq(
                            project_name=data_config.project_name,
                            dataset_name=dataset_name,
                            column_name=data_config.column_name_input,
                            table_name_source=data_config.temp_table_name_history_details,
                            table_name_for_exclusion=data_config.temp_table_name_thread_ids
                        )

    # because of big amount of data created chunks of dataframe to load data
    for df_ids_part in dataframe_chunk(df_thread_id_list, CHUNK_SIZE):
        LOGGER.info('Last record of the df chunk: %s', df_ids_part.tail(1))
        df_thread_details = pd.concat([
                                        get_one_thread(get_gmail_service(), user_id, id)
                                        for id in df_ids_part[0]
                            ], ignore_index=True)
        with TemporaryDirectory() as tmp_dir:
            filename = os.path.join(tmp_dir, data_config.temp_file_name_thread_details)
            write_dataframe_to_jsonl_file(
                df_data_to_write=df_thread_details,
                target_file_path=filename
            )
            LOGGER.info('Created file: %s', filename)
            create_or_extend_table_schema(
                gcp_project=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                full_file_location=filename,
                quoted_values_are_strings=True
            )
            load_file_into_bq(
                filename=filename,
                dataset_name=dataset_name,
                table_name=table_name,
                project_name=project_name
            )
            LOGGER.info('Loaded table: %s', table_name)


def load_from_temp_table_to_label_list(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    dataset_name = data_config.dataset_name
    project_name = data_config.project_name
    table_name = data_config.table_name_labels
    temp_table_name = data_config.temp_table_name_labels

    if does_bigquery_table_exist(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name):

        load_from_temp_table_to_actual_table(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                temp_table_name=temp_table_name,
                column_name=data_config.unique_id_column_labels
            )

    else:
        copy_bq_table(
            source_project_name=project_name,
            source_dataset_name=dataset_name,
            source_table_name=temp_table_name,
            target_project_name=project_name,
            target_dataset_name=dataset_name,
            target_table_name=table_name
        )


def load_from_temp_table_to_thread_ids_list(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    dataset_name = data_config.dataset_name
    project_name = data_config.project_name
    table_name = data_config.table_name_thread_ids
    temp_table_name = data_config.temp_table_name_thread_ids

    load_from_temp_table_to_actual_table(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                temp_table_name=temp_table_name,
                column_name=data_config.unique_id_column_thread_ids
            )


def delete_temp_table_labels(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_labels

    delete_table_from_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name
            )


def delete_temp_table_thread_ids(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_thread_ids

    delete_table_from_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name
            )


def delete_temp_table_history_details(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_history_details

    delete_table_from_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name
            )


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

gmail_label_data_to_temp_table_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_label_data_to_temp_table_etl",
    gmail_label_data_to_temp_table_etl,
    retries=5
)

gmail_thread_ids_list_to_temp_table_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_thread_ids_list_to_temp_table_etl",
    gmail_thread_ids_list_to_temp_table_etl,
    retries=5
)

gmail_history_details_to_temp_table_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_history_details_to_temp_table_etl",
    gmail_history_details_to_temp_table_etl,
    retries=5
)

gmail_thread_details_from_temp_thread_ids_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_thread_details_from_temp_thread_ids_etl",
    gmail_thread_details_from_temp_thread_ids_etl,
    retries=5
)

gmail_thread_details_from_temp_history_details_etl_task = create_python_task(
    GMAIL_DATA_DAG,
    "gmail_thread_details_from_temp_history_details_etl",
    gmail_thread_details_from_temp_history_details_etl,
    retries=5
)

delete_temp_table_labels_task = create_python_task(
    GMAIL_DATA_DAG,
    "delete_temp_table_labels",
    delete_temp_table_labels,
    retries=5
)

delete_temp_table_thread_ids_task = create_python_task(
    GMAIL_DATA_DAG,
    "delete_temp_table_thread_ids",
    delete_temp_table_thread_ids,
    retries=5
)

delete_temp_table_history_details_task = create_python_task(
    GMAIL_DATA_DAG,
    "delete_temp_table_history_details",
    delete_temp_table_history_details,
    retries=5
)

load_from_temp_table_to_label_list_task = create_python_task(
    GMAIL_DATA_DAG,
    "load_from_temp_table_to_label_list",
    load_from_temp_table_to_label_list,
    retries=5
)

load_from_temp_table_to_thread_ids_list_task = create_python_task(
    GMAIL_DATA_DAG,
    "load_from_temp_table_to_thread_ids_list",
    load_from_temp_table_to_thread_ids_list,
    retries=5
)

# pylint: disable=pointless-statement
# define dependencies between tasks in the DAG

(
    get_data_config_task >> [
        gmail_label_data_to_temp_table_etl_task,
        gmail_thread_ids_list_to_temp_table_etl_task,
        gmail_history_details_to_temp_table_etl_task
    ]
)

(
    gmail_label_data_to_temp_table_etl_task
    >> load_from_temp_table_to_label_list_task
    >> delete_temp_table_labels_task
)

(
    gmail_thread_ids_list_to_temp_table_etl_task
    >> gmail_thread_details_from_temp_thread_ids_etl_task
    >> gmail_history_details_to_temp_table_etl_task
    >> gmail_thread_details_from_temp_history_details_etl_task
    >> [
        delete_temp_table_history_details_task,
        load_from_temp_table_to_thread_ids_list_task
    ]
)

(
    load_from_temp_table_to_thread_ids_list_task
    >> delete_temp_table_thread_ids_task
)
