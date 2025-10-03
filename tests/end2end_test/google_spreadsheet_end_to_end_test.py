import logging

from data_pipeline.google_spreadsheet.cli import (
    get_multi_google_spreadsheet_config,
    main
)
from data_pipeline.google_spreadsheet.google_spreadsheet_config import MultiCsvSheetConfig
from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    GoogleSpreadsheetConfigDict
)

from data_pipeline.utils.pipeline_config import get_deployment_env

from tests.end2end_test.cli_end2end_test_helper import (
    DataPipelineCloudResource,
    check_after_test,
    clean_before_test
)

LOGGER = logging.getLogger(__name__)


def get_test_pipeline_config_dict() -> GoogleSpreadsheetConfigDict:
    multi_data_config = get_multi_google_spreadsheet_config()
    return list(multi_data_config.spreadsheets_config.values())[0]


def get_data_pipeline_cloud_resource(
    single_pipeline_config_dict: GoogleSpreadsheetConfigDict
) -> DataPipelineCloudResource:
    single_pipeline_config = MultiCsvSheetConfig.from_dict(
        single_pipeline_config_dict,
        deployment_env=get_deployment_env()
    )
    sheet_config = list(single_pipeline_config.sheets_config.values())[0]

    return DataPipelineCloudResource(
        project_name=single_pipeline_config.gcp_project,
        dataset_name=sheet_config.dataset_name,
        table_name=sheet_config.table_name,
        state_file_bucket_name=single_pipeline_config.state_file.bucket_name,
        state_file_object_name=single_pipeline_config.state_file.object_name
    )


def test_pipeline_cli():
    single_pipeline_config_dict = get_test_pipeline_config_dict()
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource(
        single_pipeline_config_dict
    )
    clean_before_test(data_pipeline_cloud_resource)
    main(['--data-pipeline-id', single_pipeline_config_dict['dataPipelineId']])
    check_after_test(data_pipeline_cloud_resource)
