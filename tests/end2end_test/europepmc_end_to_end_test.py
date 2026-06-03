from data_pipeline.europepmc.cli_europepmc import (
    get_europepmc_pipeline_config_list,
    main
)

from tests.end2end_test.cli_end2end_test_helper import (
    DataPipelineCloudResource,
    check_after_test,
    clean_before_test
)


def get_data_pipeline_cloud_resource():
    config_list = get_europepmc_pipeline_config_list()
    assert len(config_list) == 1
    config = config_list[0]
    return DataPipelineCloudResource(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name,
        state_file_bucket_name=config.state.state_file.bucket_name,
        state_file_object_name=config.state.state_file.object_name
    )


def test_cli_europepmc():
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource()
    clean_before_test(data_pipeline_cloud_resource)
    main()
    check_after_test(data_pipeline_cloud_resource)
