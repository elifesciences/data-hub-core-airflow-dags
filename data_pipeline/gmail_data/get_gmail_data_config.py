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

        self.gmail_data_table = (
            self.data_config.get("gmailData").get("table")
        )
        self.schema_file_s3_bucket = (
            self.data_config.get("gmailData").get("schemaFile", {}).get("bucket")
        )
        self.schema_file_s3_object = (
            self.data_config.get("gmailData").get("schemaFile", {}).get("object")
        )
