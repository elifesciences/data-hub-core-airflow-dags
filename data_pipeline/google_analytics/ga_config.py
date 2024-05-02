from datetime import datetime
from typing import Optional
from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)
# pylint: disable=too-few-public-methods,simplifiable-if-expression,


STORED_STATE_FORMAT = '%Y-%m-%d'


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, STORED_STATE_FORMAT)


def parse_date_or_none(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return parse_date(date_str)


class MultiGoogleAnalyticsConfig:
    def __init__(
            self,
            multi_google_analytics_config: dict,
            deployment_env,
            deployment_env_placeholder: str = '{ENV}'
    ):
        updated_config = (
            update_deployment_env_placeholder(
                multi_google_analytics_config,
                deployment_env,
                deployment_env_placeholder
            ) if deployment_env else multi_google_analytics_config
        )
        self.gcp_project = updated_config[
            "gcpProjectName"
        ]
        self.import_timestamp_field_name = updated_config[
            "importedTimestampFieldName"
        ]
        self.google_analytics_config = (
            updated_config["googleAnalyticsPipelines"]
        )


# pylint: disable=too-many-instance-attributes
class GoogleAnalyticsConfig:
    def __init__(
            self,
            config: dict,
            gcp_project: str,
            import_timestamp_field_name: str

    ):
        self.gcp_project = gcp_project
        self.import_timestamp_field_name = (
            import_timestamp_field_name
        )
        self.pipeline_id = config.get("pipelineID")
        self.default_start_date = parse_date(config["defaultStartDate"])
        self.end_date = parse_date_or_none(config.get("endDate"))
        self.dataset = config["dataset"]
        self.table = config["table"]
        self.ga_view_id = config["viewId"]
        self.dimensions = config["dimensions"]
        self.metrics = config["metrics"]
        self.state_s3_bucket_name = config["stateFile"]["bucketName"]
        self.state_s3_object_name = config["stateFile"]["objectName"]
        self.record_annotations = {
            annotation.get("recordAnnotationFieldName"):
                annotation.get("recordAnnotationFieldName")
            for annotation in config.get("recordAnnotations", [])
            if annotation.get("recordAnnotationFieldName")
        }


class ExternalTriggerConfig:
    GA_CONFIG = 'ga_config'
    DEPLOYMENT_ENV = 'dep_env'
    START_DATE = 'start_date'
    END_DATE = 'end_date'
