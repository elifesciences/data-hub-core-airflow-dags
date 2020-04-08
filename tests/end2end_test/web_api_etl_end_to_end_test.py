import os

from dags.web_api_import_controller import (
    DAG_ID,
    TARGET_DAG_ID,
    WEB_API_CONFIG_FILE_PATH_ENV_NAME,
)
from dags.web_api_import_controller import (
    DEFAULT_DEPLOYMENT_ENV_VALUE, DEPLOYMENT_ENV_ENV_NAME
)
from data_pipeline import get_yaml_file_as_dict
from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig,
    MultiWebApiConfig
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)
from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)


# pylint: disable=broad-except
def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = (
        get_etl_pipeline_cloud_resource()
    )
    trigger_run_test_pipeline(
        airflow_api, DAG_ID, TARGET_DAG_ID,
        data_pipeline_cloud_resource
    )


def get_etl_pipeline_cloud_resource():
    conf_file_path = os.getenv(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    multi_data_config = MultiWebApiConfig(data_config_dict, )
    single_web_api_config_dict = list(
        multi_data_config.web_api_config.values()
    )[0]
    single_web_api_config = WebApiConfig(
        web_api_config=single_web_api_config_dict,
        deployment_env=dep_env
    )

    return DataPipelineCloudResource(
        single_web_api_config.gcp_project,
        single_web_api_config.dataset_name,
        single_web_api_config.table_name,
        single_web_api_config.state_file_bucket_name,
        single_web_api_config.state_file_object_name
    )
