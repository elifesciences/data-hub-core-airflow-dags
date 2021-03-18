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

        self.stage_file_name_labels = (
            self.data_config.get("gmailLabelData").get("stageFileName")
        )
        self.table_name_labels = (
            self.data_config.get("gmailLabelData").get("table")
        )

        self.stage_file_name_link_ids = (
            self.data_config.get("gmailLinkIdsData").get("stageFileName")
        )
        self.table_name_link_ids = (
            self.data_config.get("gmailLinkIdsData").get("table")
        )
