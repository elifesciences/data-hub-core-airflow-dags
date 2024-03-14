import os

from dags.google_analytics_data_import_pipeline import (
    DAG_ID,
    GOOGLE_ANALYTICS_CONFIG_FILE_PATH_ENV_NAME,
    DEPLOYMENT_ENV_ENV_NAME,
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.google_analytics.ga_config import (
    MultiGoogleAnalyticsConfig,
    GoogleAnalyticsConfig
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)
from tests.end2end_test import (
    DataPipelineCloudResource,
    trigger_run_test_pipeline
)


def get_etl_pipeline_cloud_resource():
    conf_file_path = os.getenv(
        GOOGLE_ANALYTICS_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME
    )
    multi_data_config = MultiGoogleAnalyticsConfig(
        data_config_dict, deployment_env=dep_env
    )
    single_ga_config_dict = (
        multi_data_config.google_analytics_config[0]
    )
    single_ga_config = GoogleAnalyticsConfig(
        single_ga_config_dict,
        gcp_project=multi_data_config.gcp_project,
        import_timestamp_field_name=(
            multi_data_config.import_timestamp_field_name
        )
    )

    return DataPipelineCloudResource(
        single_ga_config.gcp_project,
        single_ga_config.dataset,
        single_ga_config.table,
        single_ga_config.state_s3_bucket_name,
        single_ga_config.state_s3_object_name
    )


# pylint: disable=broad-except
def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
    ga_data_pipeline_cloud_resource = (
        get_etl_pipeline_cloud_resource()
    )
    config = {
        "start_date": "2020-05-01",
        "end_date": "2020-05-02"
    }
    trigger_run_test_pipeline(
        airflow_api,
        ga_data_pipeline_cloud_resource,
        DAG_ID,
        dag_trigger_conf=config
    )
