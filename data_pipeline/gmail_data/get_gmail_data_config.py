from dataclasses import dataclass
import logging
from typing import Any, Optional
from data_pipeline.utils.pipeline_config import (
    ConfigKeys,
    update_deployment_env_placeholder
)

LOGGER = logging.getLogger(__name__)


def get_gmail_config_id(gmail_config_props: dict) -> Optional[Any]:
    return gmail_config_props.get(ConfigKeys.DATA_PIPELINE_CONFIG_ID)


# pylint: disable=too-many-instance-attributes,too-many-arguments
# pylint: disable=too-many-locals, too-few-public-methods
@dataclass(frozen=True)
class MultiGmailDataConfig:
    project_name: str
    dataset_name: str
    gmail_data_config: dict

    @staticmethod
    def from_dict(multi_gmail_data_config: dict) -> 'MultiGmailDataConfig':
        project_name = multi_gmail_data_config['projectName']
        dataset_name = multi_gmail_data_config['datasetName']
        gmail_data_config = {
            get_gmail_config_id(gmail): {
                **gmail,
                ConfigKeys.DATA_PIPELINE_CONFIG_ID: get_gmail_config_id(gmail),
                'projectName': project_name,
                'datasetName': dataset_name
            }
            for gmail in multi_gmail_data_config['gmailData']
        }
        return MultiGmailDataConfig(
            project_name=project_name,
            dataset_name=dataset_name,
            gmail_data_config=gmail_data_config
        )


# pylint: disable=too-few-public-methods, too-many-instance-attributes
@dataclass(frozen=True)
class GmailDataConfig:
    project_name: str
    dataset_name: str
    data_pipeline_id: str
    table_name_labels: str
    temp_table_name_labels: str
    unique_id_column_labels: str
    table_name_thread_ids: str
    temp_table_name_thread_ids: str
    unique_id_column_thread_ids: str
    temp_table_name_history_details: str
    table_name_thread_details: str
    column_name_input: str
    column_name_history_check: str
    array_name_in_thread_details: str
    array_column_name: str
    gmail_secret_file_env_name: str

    @staticmethod
    def from_dict(
        data_config: dict,
        deployment_env: Optional[str] = None,
        env_placeholder: str = '{ENV}'
    ) -> 'GmailDataConfig':
        LOGGER.info('deployment_env: %s', deployment_env)
        LOGGER.info('env_placeholder: %s', env_placeholder)
        gmail_data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env,
            env_placeholder
        ) if deployment_env else data_config

        LOGGER.debug('gmail_data_config: %s', gmail_data_config)

        project_name = gmail_data_config['projectName']
        dataset_name = gmail_data_config['datasetName']

        data_pipeline_id = gmail_data_config['dataPipelineId']

        # label list
        table_name_labels = (
            gmail_data_config['gmailLabelData'].get('table')
        )
        temp_table_name_labels = (
            gmail_data_config['gmailLabelData'].get('tempTable')
        )
        unique_id_column_labels = (
            gmail_data_config['gmailLabelData'].get('uniqueIdColumn')
        )

        # message-thread ids list
        table_name_thread_ids = (
            gmail_data_config['gmailLinkIdsData'].get('table')
        )
        temp_table_name_thread_ids = (
            gmail_data_config['gmailLinkIdsData'].get('tempTable')
        )
        unique_id_column_thread_ids = (
            gmail_data_config['gmailLinkIdsData'].get('uniqueIdColumn')
        )

        # history details
        temp_table_name_history_details = (
            gmail_data_config['gmailHistoryData'].get('tempTable')
        )

        # thread details
        table_name_thread_details = (
            gmail_data_config['gmailThreadData'].get('table')
        )
        column_name_input = (
            gmail_data_config['gmailThreadData'].get('inputColumn')
        )
        column_name_history_check = (
            gmail_data_config['gmailThreadData'].get('historyCheckColumn')
        )
        array_name_in_thread_details = (
            gmail_data_config['gmailThreadData'].get('array_name_in_table')
        )
        array_column_name = (
            gmail_data_config['gmailThreadData'].get('array_column_name')
        )

        # secret
        gmail_secret_file_env_name = gmail_data_config['gmailSecretFileEnvName']

        return GmailDataConfig(
            project_name=project_name,
            dataset_name=dataset_name,
            data_pipeline_id=data_pipeline_id,
            table_name_labels=table_name_labels,
            temp_table_name_labels=temp_table_name_labels,
            unique_id_column_labels=unique_id_column_labels,
            table_name_thread_ids=table_name_thread_ids,
            temp_table_name_thread_ids=temp_table_name_thread_ids,
            unique_id_column_thread_ids=unique_id_column_thread_ids,
            temp_table_name_history_details=temp_table_name_history_details,
            table_name_thread_details=table_name_thread_details,
            column_name_input=column_name_input,
            column_name_history_check=column_name_history_check,
            array_name_in_thread_details=array_name_in_thread_details,
            array_column_name=array_column_name,
            gmail_secret_file_env_name=gmail_secret_file_env_name
        )
