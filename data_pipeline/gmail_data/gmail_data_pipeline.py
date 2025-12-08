import logging
import os
from tempfile import TemporaryDirectory

import pandas as pd

from googleapiclient.discovery import Resource

from data_pipeline.gmail_data.get_gmail_data import (
    GmailCredentials,
    get_gmail_service_via_refresh_token,
    get_label_list,
    refresh_gmail_token,
    write_dataframe_to_jsonl_file,
    get_link_message_thread_ids
)
from data_pipeline.gmail_data.get_gmail_data_config import (
    GmailDataConfig,
    MultiGmailDataConfig
)
from data_pipeline.utils.data_store.bq_data_service import (
    copy_bq_table,
    create_or_extend_table_schema,
    delete_table_from_bq,
    does_bigquery_table_exist,
    load_file_into_bq,
    load_from_temp_table_to_actual_table
)
from data_pipeline.utils.pipeline_config import (
    get_env_var_or_use_default,
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class GmailPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'GMAIL_DATA_CONFIG_FILE_PATH'


def get_multi_gmail_data_config() -> MultiGmailDataConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        GmailPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        MultiGmailDataConfig.from_dict
    )


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


def get_gmail_credentials(data_config: GmailDataConfig) -> GmailCredentials:
    secret_file = get_env_var_or_use_default(data_config.gmail_secret_file_env_name)
    LOGGER.info("gmail secret file name %s", secret_file)
    return GmailCredentials(secret_file)


def get_gmail_user_id(data_config: GmailDataConfig) -> str:
    gmail_credentials = get_gmail_credentials(data_config)
    user_id = gmail_credentials.user_id
    LOGGER.info("gmail user_id: %s", user_id)
    return user_id


def load_bq_table_from_df(
    project_name: str,
    dataset_name: str,
    table_name: str,
    df_data_to_write: pd.DataFrame
):

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


def delete_temp_table_labels(data_config: GmailDataConfig):
    project_name = data_config.project_name
    dataset_name = data_config.dataset_name
    table_name = data_config.temp_table_name_labels

    delete_table_from_bq(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=table_name
    )


def gmail_label_data_to_temp_table_etl(data_config: GmailDataConfig):
    user_id = get_gmail_user_id(data_config)

    load_bq_table_from_df(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.temp_table_name_labels,
        df_data_to_write=get_label_list(get_gmail_service(data_config),  user_id)
    )


def load_from_temp_table_to_label_list(data_config: GmailDataConfig):
    dataset_name = data_config.dataset_name
    project_name = data_config.project_name
    table_name = data_config.table_name_labels
    temp_table_name = data_config.temp_table_name_labels

    if does_bigquery_table_exist(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=table_name
    ):
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


def gmail_thread_ids_list_to_temp_table_etl(data_config: GmailDataConfig):
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
