from data_pipeline.generic_web_api.cli import get_multi_web_api_config, main
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)

from tests.end2end_test.cli_end2end_test_helper import (
    DataPipelineCloudResource,
    check_after_test,
    clean_before_test
)


def get_test_web_api_config_dict() -> WebApiConfigDict:
    multi_data_config = get_multi_web_api_config()
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


def test_web_api_pipeline_cli():
    single_web_api_config_dict = get_test_web_api_config_dict()
    data_pipeline_cloud_resource = (
        get_etl_pipeline_cloud_resource(single_web_api_config_dict)
    )
    clean_before_test(data_pipeline_cloud_resource)
    main(['--data-pipeline-id', single_web_api_config_dict['dataPipelineId']])
    check_after_test(data_pipeline_cloud_resource)
