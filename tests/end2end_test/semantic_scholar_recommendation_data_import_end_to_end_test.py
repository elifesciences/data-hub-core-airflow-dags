from data_pipeline.semantic_scholar.cli_semantic_scholar_recommendation import (
    get_pipeline_config,
    main
)

from tests.end2end_test.cli_end2end_test_helper import (
    DataPipelineCloudResource,
    check_after_test,
    clean_before_test
)


def get_data_pipeline_cloud_resource() -> DataPipelineCloudResource:
    config = get_pipeline_config()

    return DataPipelineCloudResource(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name
    )


def test_cli_semantic_scholar_recommendation():
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource()
    clean_before_test(data_pipeline_cloud_resource)
    main([])
    check_after_test(data_pipeline_cloud_resource)
