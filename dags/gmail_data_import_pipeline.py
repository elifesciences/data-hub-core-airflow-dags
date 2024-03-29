# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory
from googleapiclient.discovery import Resource
from googleapiclient import errors
import pandas as pd

from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
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
    GmailDataConfig
)

from data_pipeline.gmail_data.get_gmail_data import (
    GmailCredentials,
    refresh_gmail_token,
    get_gmail_service_via_refresh_token,
    get_label_list,
    write_dataframe_to_jsonl_file,
    get_link_message_thread_ids,
    get_gmail_user_profile,
    get_gmail_history_details,
    get_one_thread,
    dataframe_chunk
)

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"

DAG_ID = "Gmail_Data_Import_Pipeline"

GMAIL_THREAD_DETAILS_CHUNK_SIZE_ENV_NAME = "GMAIL_THREAD_DETAILS_CHUNK_SIZE"
DEFAULT_GMAIL_THREAD_DETAILS_CHUNK_SIZE = "100"

LOGGER = logging.getLogger(__name__)


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_data_config(context):
    data_config_dict = context["dag_run"].conf
    LOGGER.info('data_config_dict: %s', data_config_dict)
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV)
    LOGGER.info("deployment_env: %s", deployment_env)
    data_config = GmailDataConfig(
        data_config=data_config_dict,
        deployment_env=deployment_env
    )
    LOGGER.info('data_config: %s', data_config)
    return data_config


def get_gmail_credentials(data_config: GmailDataConfig) -> GmailCredentials:
    secret_file = get_env_var_or_use_default(data_config.gmail_secret_file_env_name)
    LOGGER.info("gmail secret file name %s", secret_file)
    return GmailCredentials(secret_file)


def get_gmail_user_id(data_config: GmailDataConfig) -> str:
    gmail_credentials = get_gmail_credentials(data_config)
    user_id = gmail_credentials.user_id
    LOGGER.info("gmail user_id: %s", user_id)
    return user_id


def get_gmail_service(data_config: GmailDataConfig) -> Resource:
    gmail_credentials = get_gmail_credentials(data_config)
    return get_gmail_service_via_refresh_token(
        refresh_gmail_token(
            client_id=gmail_credentials.client_id,
            client_secret=gmail_credentials.client_secret,
            refresh_token=gmail_credentials.refresh_token,
            scopes=GMAIL_SCOPES
        )
    )


def get_gmail_thread_details_chunk_size() -> int:
    chunk_size = int(get_env_var_or_use_default(
        GMAIL_THREAD_DETAILS_CHUNK_SIZE_ENV_NAME,
        DEFAULT_GMAIL_THREAD_DETAILS_CHUNK_SIZE
    ))
    LOGGER.info("Thread details chunk size is :%s", chunk_size)
    return chunk_size


def load_bq_table_from_df(
        project_name: str,
        dataset_name: str,
        table_name: str,
        df_data_to_write: pd.DataFrame):

    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, 'tmp_file.json')

        if not df_data_to_write.empty:
            write_dataframe_to_jsonl_file(
                df_data_to_write,
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
        else:
            LOGGER.info('No updates found for the table: %s', table_name)


def gmail_label_data_to_temp_table_etl(**kwargs):
    data_config = get_data_config(kwargs)
    user_id = get_gmail_user_id(data_config)

    load_bq_table_from_df(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.temp_table_name_labels,
        df_data_to_write=get_label_list(get_gmail_service(data_config),  user_id)
    )


def gmail_thread_ids_list_to_temp_table_etl(**kwargs):
    data_config = get_data_config(kwargs)
    user_id = get_gmail_user_id(data_config)

    load_bq_table_from_df(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.temp_table_name_thread_ids,
        df_data_to_write=get_link_message_thread_ids(
            get_gmail_service(data_config),
            user_id
        )
    )


def gmail_history_details_to_temp_table_etl(**kwargs):
    data_config = get_data_config(kwargs)
    user_id = get_gmail_user_id(data_config)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name

    try:
        start_id = get_max_value_from_bq_table(
                        project_name=project_name,
                        dataset_name=dataset_name,
                        column_name=data_config.column_name_history_check,
                        table_name=data_config.table_name_thread_details
                    )

        LOGGER.info('Get history start_id from BigQuery: %s', start_id)

        load_bq_table_from_df(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=data_config.temp_table_name_history_details,
            df_data_to_write=get_gmail_history_details(
                get_gmail_service(data_config),
                user_id,
                str(start_id)
            )
        )
    except errors.HttpError:
        start_id = get_gmail_user_profile(
            get_gmail_service(data_config),
            get_gmail_user_id(data_config)
        )["historyId"]

        LOGGER.info('Get history start_id from user profile: %s', start_id)

        load_bq_table_from_df(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=data_config.temp_table_name_history_details,
            df_data_to_write=get_gmail_history_details(
                get_gmail_service(data_config),
                user_id,
                str(start_id)
            )
        )


def gmail_thread_details_from_temp_thread_ids_etl(**kwargs):
    data_config = get_data_config(kwargs)
    user_id = get_gmail_user_id(data_config)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.table_name_thread_details

    if does_bigquery_table_exist(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name
    ):
        df_thread_id_list = get_distinct_values_from_bq(
                            project_name=project_name,
                            dataset_name=dataset_name,
                            column_name=data_config.column_name_input,
                            table_name_source=data_config.temp_table_name_thread_ids,
                            table_name_for_exclusion=table_name,
                            array_table_name=data_config.array_name_in_thread_details,
                            array_column_for_exclusion=data_config.array_column_name,
                        )
    else:
        df_thread_id_list = get_distinct_values_from_bq(
                            project_name=project_name,
                            dataset_name=dataset_name,
                            column_name=data_config.column_name_input,
                            table_name_source=data_config.temp_table_name_thread_ids
                        )

    # because of big amount of data created chunks of dataframe to load data
    for df_ids_part in dataframe_chunk(df_thread_id_list, get_gmail_thread_details_chunk_size()):
        LOGGER.info('Last record of the df chunk: %s', df_ids_part.tail(1))
        df_thread_details = pd.concat([
                                        get_one_thread(get_gmail_service(data_config), user_id, id)
                                        for id in df_ids_part[0]
                            ], ignore_index=True)

        load_bq_table_from_df(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            df_data_to_write=df_thread_details
        )


def gmail_thread_details_from_temp_history_details_etl(**kwargs):
    data_config = get_data_config(kwargs)
    user_id = get_gmail_user_id(data_config)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.table_name_thread_details

    if does_bigquery_table_exist(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=data_config.temp_table_name_history_details
    ):
        df_thread_id_list = get_distinct_values_from_bq(
                                project_name=data_config.project_name,
                                dataset_name=dataset_name,
                                column_name=data_config.column_name_input,
                                table_name_source=data_config.temp_table_name_history_details,
                                table_name_for_exclusion=data_config.temp_table_name_thread_ids
                            )

        # because of big amount of data created chunks of dataframe to load data
        for df_ids_part in dataframe_chunk(
            df_thread_id_list,
            get_gmail_thread_details_chunk_size()
        ):
            LOGGER.info('Last record of the df chunk: %s', df_ids_part.tail(1))
            df_thread_details = pd.concat([
                                            get_one_thread(
                                                get_gmail_service(data_config),
                                                user_id, id
                                            )
                                            for id in df_ids_part[0]
                                ], ignore_index=True)
            load_bq_table_from_df(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                df_data_to_write=df_thread_details
            )


def load_from_temp_table_to_label_list(**kwargs):
    data_config = get_data_config(kwargs)
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
    data_config = get_data_config(kwargs)
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
    data_config = get_data_config(kwargs)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_labels

    delete_table_from_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name
            )


def delete_temp_table_thread_ids(**kwargs):
    data_config = get_data_config(kwargs)
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_thread_ids

    delete_table_from_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name
            )


def delete_temp_table_history_details(**kwargs):
    data_config = get_data_config(kwargs)
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
    schedule=None,
    dagrun_timeout=timedelta(days=1)
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
