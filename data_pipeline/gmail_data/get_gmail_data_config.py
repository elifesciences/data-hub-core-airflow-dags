from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class GmailGetDataConfig:

    def __init__(self, data_config: dict,
                 deployment_env: str,
                 environment_placeholder: str = "{ENV}"
                 ):
        self.data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env,
            environment_placeholder
        )
        self.project_name = self.data_config.get("projectName")
        self.dataset = self.data_config.get("dataset")

        self.stage_file_name_labels = (
            self.data_config.get("gmailLabelData").get("stageFileName")
        )
        self.table_name_labels = (
            self.data_config.get("gmailLabelData").get("table")
        )
        self.schema_file_s3_bucket_labels = (
            self.data_config.get("gmailLabelData").get("schemaFile", {}).get("bucket")
        )
        self.schema_file_s3_object_labels = (
            self.data_config.get("gmailLabelData").get("schemaFile", {}).get("object")
        )
