from data_pipeline.utils.common.common_csv_util import (
    update_deployment_env_placeholder
)


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class CrossRefImportDataPipelineConfig:
    MESSAGE_KEY = 'message'
    EVENT_KEY = 'events'

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
        self.table = self.data_config.get("table")
        self.imported_timestamp_field = (
            self.data_config.get("importedTimestampField")
        )
        self.state_file_name_key = (
            self.data_config.get("stateFile").get("objectName")
        )
        self.state_file_bucket = (
            self.data_config.get("stateFile").get("bucket")
        )
        self.schema_file_s3_bucket = (
            self.data_config.get("schemaFile", {}).get("bucket")
        )
        self.schema_file_object_name = self.data_config.get(
            "schemaFile", {}).get("objectName")
        self.publisher_ids = self.data_config.get("publisherIdPrefixes")
        self.crossref_event_base_url = (
            self.data_config.get("CrossrefEventBaseUrl")
        )


class ExternalTriggerConfig:
    UNTIL_TIME_COLLECTED_PARAM_KEY = "until_collected_date"
    CURRENT_TIMESTAMP_PARAM_KEY = "current_timestamp"
    LATEST_DOWNLOAD_DATE_PARAM_KEY = "latest_download_date"
    BQ_DATASET_PARAM_KEY = "dataset"
    BQ_TABLE_PARAM_KEY = "table"


class DownloadStateConfig:
    DATE = "date"
    DOI_PREFIX = "doi_prefix"
