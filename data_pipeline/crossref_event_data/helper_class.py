# pylint: disable=too-few-public-methods
class CrossRefImportDataPipelineConfig:

    # pylint: disable=too-many-instance-attributes, too-few-public-methods
    def __init__(self, data_config):
        self.data_config = data_config
        self.project_name = self.data_config.get("projectName")
        self.dataset = self.data_config.get("dataset")
        self.table = self.data_config.get("table")
        self.imported_timestamp_field = (
            self.data_config.get("importedTimestampField")
        )
        self.schema_file_s3_bucket = (
            self.data_config.get("schemaFile").get("bucket")
        )
        self.state_file_name_key = (
            self.data_config.get("stateFile").get("objectName")
        )
        self.state_file_bucket = (
            self.data_config.get("stateFile").get("bucket")
        )
        self.message_key = self.data_config.get("messageKey")
        self.publisher_ids = self.data_config.get("publisherIdPrefixes")
        self.crossref_event_base_url = (
            self.data_config.get("CrossrefEventBaseUrl")
        )
        self.event_key = self.data_config.get("eventKey")
        self.schema_file_object_name = self.data_config.get("schemaFile").get(
            "objectName"
        )
        self.deployment_env_based_name_modification = self.data_config.get(
            "deploymentEnvBasedObjectNameModification"
        )

    def get_dataset_name_based_on_deployment_env(self, deployment_env):
        dataset_name = self.dataset
        if self.deployment_env_based_name_modification == "replace":
            dataset_name = deployment_env
        elif self.deployment_env_based_name_modification == "append":
            dataset_name = "_".join([self.dataset, deployment_env])

        return dataset_name

    def get_state_object_name_based_on_deployment_env(self, deployment_env):
        download_state_file = (
            "_".join([self.state_file_name_key, deployment_env])
            if self.deployment_env_based_name_modification
            in {"replace", "append"}
            else self.state_file_name_key
        )

        return download_state_file

    def modify_config_based_on_env(self, deployment_env):

        self.data_config["DATASET"] = (
            self.get_dataset_name_based_on_deployment_env(
                deployment_env
            )
        )
        self.data_config["STATE_FILE"][
            "OBJECT_NAME"
        ] = self.get_state_object_name_based_on_deployment_env(deployment_env)
        return self.data_config


class ExternalTriggerConfig:
    UNTIL_TIME_COLLECTED_PARAM_KEY = "until_collected_date"
    CURRENT_TIMESTAMP_PARAM_KEY = "current_timestamp"
    LATEST_DOWNLOAD_DATE_PARAM_KEY = "latest_download_date"
    BQ_DATASET_PARAM_KEY = "dataset"
    BQ_TABLE_PARAM_KEY = "table"


class DownloadStateConfig:
    DATE = "date"
    DOI_PREFIX = "doi_prefix"
