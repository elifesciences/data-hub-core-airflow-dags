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
class MultiGmailDataConfig:
    def __init__(
            self,
            multi_gmail_data_config: dict,
    ):
        self.project_name = multi_gmail_data_config["projectName"]
        self.dataset_name = multi_gmail_data_config["datasetName"]
        self.gmail_data_config = {
            ind: {
                **gmail,
                ConfigKeys.DATA_PIPELINE_CONFIG_ID: get_gmail_config_id(gmail),
                "projectName": self.project_name,
                "datasetName": self.dataset_name
            }
            for ind, gmail in enumerate(
                multi_gmail_data_config["gmailData"]
            )
        }


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class GmailDataConfig:
    def __init__(
        self,
        data_config: dict,
        project_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        deployment_env: Optional[str] = None,
        env_placeholder: str = "{ENV}"
    ):
        LOGGER.info("deployment_env: %s", deployment_env)
        LOGGER.info("env_placeholder: %s", env_placeholder)
        gmail_data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env,
            env_placeholder
        ) if deployment_env else data_config

        LOGGER.info("gmail_data_config: %s", gmail_data_config)

        self.project_name = gmail_data_config.get("projectName", project_name)
        self.dataset_name = gmail_data_config.get("datasetName", dataset_name)

        self.data_pipeline_id = gmail_data_config.get("dataPipelineId")

        # label list
        self.table_name_labels = (
            gmail_data_config.get("gmailLabelData").get("table")
        )
        self.temp_table_name_labels = (
            gmail_data_config.get("gmailLabelData").get("tempTable")
        )
        self.unique_id_column_labels = (
            gmail_data_config.get("gmailLabelData").get("uniqueIdColumn")
        )

        # message-thread ids list
        self.table_name_thread_ids = (
            gmail_data_config.get("gmailLinkIdsData").get("table")
        )
        self.temp_table_name_thread_ids = (
            gmail_data_config.get("gmailLinkIdsData").get("tempTable")
        )
        self.unique_id_column_thread_ids = (
            gmail_data_config.get("gmailLinkIdsData").get("uniqueIdColumn")
        )

        # history details
        self.temp_table_name_history_details = (
            gmail_data_config.get("gmailHistoryData").get("tempTable")
        )

        # thread details
        self.table_name_thread_details = (
            gmail_data_config.get("gmailThreadData").get("table")
        )
        self.column_name_input = (
            gmail_data_config.get("gmailThreadData").get("inputColumn")
        )
        self.column_name_history_check = (
            gmail_data_config.get("gmailThreadData").get("historyCheckColumn")
        )
        self.array_name_in_thread_details = (
            gmail_data_config.get("gmailThreadData").get("array_name_in_table")
        )
        self.array_column_name = (
            gmail_data_config.get("gmailThreadData").get("array_column_name")
        )

        # secret
        self.gmail_secret_file_env_name = (
            gmail_data_config.get("gmailSecretFileEnvName")
        )

    def __repr__(self):
        return repr(vars(self))
