import logging

from data_pipeline.gmail_data.get_gmail_data_config import GmailDataConfig
from data_pipeline.gmail_data.gmail_data_pipeline import (
    get_multi_gmail_data_config
)
from data_pipeline.gmail_data.cli import main

from tests.end2end_test.cli_end2end_test_helper import (
    DataPipelineCloudResource,
    check_after_test,
    clean_before_test
)

LOGGER = logging.getLogger(__name__)


def get_test_gmail_data_pipeline_config() -> GmailDataConfig:
    multi_data_config = get_multi_gmail_data_config()
    return GmailDataConfig.from_dict(
        list(multi_data_config.gmail_data_config.values())[0]
    )


def get_data_pipeline_cloud_resource(
    data_config: GmailDataConfig
) -> DataPipelineCloudResource:

    return DataPipelineCloudResource(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.table_name_thread_ids,
        state_file_bucket_name=None,
        state_file_object_name=None
    )


def test_gmail_data_pipeline_cli():
    test_gmail_data_pipeline_config = get_test_gmail_data_pipeline_config()
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource(
        test_gmail_data_pipeline_config
    )
    clean_before_test(data_pipeline_cloud_resource)
    main(['--data-pipeline-id', test_gmail_data_pipeline_config.data_pipeline_id])
    check_after_test(data_pipeline_cloud_resource)
