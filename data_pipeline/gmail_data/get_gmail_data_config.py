from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class GmailGetDataConfig:

    def __init__(self, data_config: dict,
                 deployment_env: str,
                 env_placeholder: str = "{ENV}"
                 ):
        self.data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env,
            env_placeholder
        )
        self.project_name = self.data_config.get("projectName")
        self.dataset_name = self.data_config.get("datasetName")

        # label list
        self.table_name_labels = (
            self.data_config.get("gmailLabelData").get("table")
        )
        self.temp_table_name_labels = (
            self.data_config.get("gmailLabelData").get("tempTable")
        )
        self.unique_id_column_labels = (
            self.data_config.get("gmailLabelData").get("uniqueIdColumn")
        )

        # message-thread ids list
        self.table_name_thread_ids = (
            self.data_config.get("gmailLinkIdsData").get("table")
        )
        self.temp_table_name_thread_ids = (
            self.data_config.get("gmailLinkIdsData").get("tempTable")
        )
        self.unique_id_column_thread_ids = (
            self.data_config.get("gmailLinkIdsData").get("uniqueIdColumn")
        )

        # history details
        self.temp_table_name_history_details = (
            self.data_config.get("gmailHistoryData").get("tempTable")
        )

        # thread details
        self.table_name_thread_details = (
            self.data_config.get("gmailThreadData").get("table")
        )
        self.column_name_input = (
            self.data_config.get("gmailThreadData").get("inputColumn")
        )
        self.column_name_history_check = (
            self.data_config.get("gmailThreadData").get("historyCheckColumn")
        )

    def __repr__(self):
        return repr(vars(self))
