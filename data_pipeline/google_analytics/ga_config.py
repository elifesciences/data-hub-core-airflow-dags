import dataclasses
from datetime import datetime
from typing import Mapping, Optional, Sequence

from dateutil.relativedelta import relativedelta

from data_pipeline.utils.data_store.google_analytics import DEFAULT_PAGE_SIZE
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
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
        deployment_env: Optional[str] = None,
        deployment_env_placeholder: str = '{ENV}'
    ):
        updated_config = (
            update_deployment_env_placeholder(
                multi_google_analytics_config,
                deployment_env,
                deployment_env_placeholder
            ) if deployment_env else multi_google_analytics_config
        )
        default_config_dict = updated_config.get('defaultConfig', {})
        self.default_airflow_config = AirflowConfig.from_optional_dict(
            default_config_dict.get('airflow')
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


def parse_date_delta_dict(date_interval_dict: dict) -> relativedelta:
    return relativedelta(
        days=date_interval_dict.get('days', 0),
        months=date_interval_dict.get('months', 0),
        years=date_interval_dict.get('years', 0)
    )


# pylint: disable=too-many-instance-attributes
@dataclasses.dataclass(frozen=True)
class GoogleAnalyticsConfig:
    gcp_project: str
    import_timestamp_field_name: str
    default_start_date: datetime
    end_date: Optional[datetime]
    dataset: str
    table: str
    ga_view_id: str
    dimensions: Sequence[str]
    metrics: Sequence[str]
    state_s3_bucket_name: str
    state_s3_object_name: str
    record_annotations: Mapping[str, str]
    log_response: bool = False
    pipeline_id: Optional[str] = None
    page_size: int = DEFAULT_PAGE_SIZE
    batch_date_interval: relativedelta = relativedelta()

    @staticmethod
    def from_dict(
        config: dict,
        gcp_project: str,
        import_timestamp_field_name: str
    ) -> 'GoogleAnalyticsConfig':
        return GoogleAnalyticsConfig(
            gcp_project=gcp_project,
            import_timestamp_field_name=import_timestamp_field_name,
            pipeline_id=config.get("pipelineID"),
            default_start_date=parse_date(config["defaultStartDate"]),
            end_date=parse_date_or_none(config.get("endDate")),
            batch_date_interval=parse_date_delta_dict(config.get('batchDateInterval', {})),
            dataset=config["dataset"],
            table=config["table"],
            ga_view_id=config["viewId"],
            dimensions=config["dimensions"],
            metrics=config["metrics"],
            state_s3_bucket_name=config["stateFile"]["bucketName"],
            state_s3_object_name=config["stateFile"]["objectName"],
            record_annotations={
                annotation['recordAnnotationFieldName']: annotation['recordAnnotationValue']
                for annotation in config.get("recordAnnotations", [])
                if annotation.get("recordAnnotationFieldName")
            },
            log_response=config.get('logResponse', False),
            page_size=config.get('pageSize', DEFAULT_PAGE_SIZE)
        )


class ExternalTriggerConfig:
    GA_CONFIG = 'ga_config'
    DEPLOYMENT_ENV = 'dep_env'
    START_DATE = 'start_date'
    END_DATE = 'end_date'
