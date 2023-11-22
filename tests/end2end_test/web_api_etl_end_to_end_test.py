from dags.web_api_data_import_pipeline import (
    WEB_API_CONFIG_FILE_PATH_ENV_NAME,
    get_dag_id_for_web_api_config_dict
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.utils.pipeline_config import get_pipeline_config_for_env_name_and_config_parser
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


def get_test_web_api_config_dict() -> WebApiConfigDict:
    multi_data_config = get_pipeline_config_for_env_name_and_config_parser(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME,
        MultiWebApiConfig
    )
    return list(
        multi_data_config.web_api_config.values()
    )[0]


def get_etl_pipeline_cloud_resource(web_api_config: WebApiConfigDict) -> DataPipelineCloudResource:
    single_web_api_config = WebApiConfig.from_dict(web_api_config=web_api_config)

    return DataPipelineCloudResource(
        single_web_api_config.gcp_project,
        single_web_api_config.dataset_name,
        single_web_api_config.table_name,
        single_web_api_config.state_file_bucket_name,
        single_web_api_config.state_file_object_name
    )


def test_dag_runs_data_imported():
    single_web_api_config = get_test_web_api_config_dict()
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = (
        get_etl_pipeline_cloud_resource(single_web_api_config)
    )
    trigger_run_test_pipeline(
        airflow_api=airflow_api,
        dag_id=get_dag_id_for_web_api_config_dict(single_web_api_config),
        pipeline_cloud_resource=data_pipeline_cloud_resource
    )
