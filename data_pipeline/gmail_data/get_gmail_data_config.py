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
        self.dataset = self.data_config.get("dataset")

        # label list
        self.stage_file_name_labels = (
            self.data_config.get("gmailLabelData").get("stageFileName")
        )
        self.table_name_labels = (
            self.data_config.get("gmailLabelData").get("table")
        )

        # message-thread ids list
        self.stage_file_name_link_ids = (
            self.data_config.get("gmailLinkIdsData").get("stageFileName")
        )
        self.table_name_link_ids = (
            self.data_config.get("gmailLinkIdsData").get("table")
        )

        # history details
        self.stage_file_name_history_details = (
            self.data_config.get("gmailHistoryData").get("stageFileName")
        )
        self.table_name_history_details = (
            self.data_config.get("gmailHistoryData").get("table")
        )

        # thread details
        self.stage_file_name_thread_details = (
            self.data_config.get("gmailThreadData").get("stageFileName")
        )
        self.table_name_thread_details = (
            self.data_config.get("gmailThreadData").get("table")
        )
        self.table_name_list_of_thread_ids = (
            self.data_config.get("gmailThreadData").get("listThreadId").get("table")
        )
        self.column_name_list_of_thread_ids = (
            self.data_config.get("gmailThreadData").get("listThreadId").get("column")
        )

    def __repr__(self):
        return repr(vars(self))
