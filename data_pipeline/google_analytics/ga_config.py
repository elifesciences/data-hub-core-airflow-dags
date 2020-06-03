from data_pipeline.utils import update_deployment_env_placeholder
# pylint: disable=too-few-public-methods,simplifiable-if-expression,


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
        self.gcp_project = updated_config.get(
            "gcpProjectName"
        )
        self.import_timestamp_field_name = updated_config.get(
            "importedTimestampFieldName"
        )
        self.google_analytics_config = (
            updated_config.get("googleAnalyticsPipelines")
        )


# pylint: disable=too-many-instance-attributes
class GoogleAnalyticsConfig:
    def __init__(
            self,
            config: dict,
            gcp_project: str = None,
    ):
        self.gcp_project = gcp_project
        self.import_timestamp_field_name = config.get(
            'importedTimestampFieldName'
        )
        self.pipeline_id = config.get("pipelineID")
        self.default_start_date_as_string = config.get("defaultStartDate")
        self.dataset = config.get("dataset")
        self.table = config.get("table")
        self.ga_view_id = config.get("viewId")
        self.dimensions = config.get("dimensions")
        self.metrics = config.get("metrics")
        self.state_s3_bucket_name = config.get(
            "stateFile", {}).get("bucketName")
        self.state_s3_object_name = config.get(
            "stateFile", {}).get("objectName")
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
