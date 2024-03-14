from unittest.mock import patch
import pytest
from botocore.exceptions import ClientError
from data_pipeline.google_analytics import etl_state as etl_state_module
from data_pipeline.google_analytics.etl_state import (
    get_stored_state,
)
from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig


GA_CONFIG = {
    'gcpProjectName': 'elife-data-pipeline',
    'dataPipelineId': 'ga_pipeline_id',
    'dataset': 'dataset'
}


IMPORTED_TIMESTAMP_FIELD_NAME = 'imported_timestamp'


@pytest.fixture(name="mock_download_s3_json_object")
def _download_s3_json_object():
    with patch.object(
            etl_state_module, 'download_s3_object_as_string'
    ) as mock:
        yield mock


class TestGetStoredGAProcessingState:
    # pylint: disable=unused-argument
    def test_get_state_no_state_file_in_bucket(
            self, mock_download_s3_json_object
    ):
        s3_client_error_response = {
            'Error': {
                'Code': 'NoSuchKey'
            }
        }
        mock_download_s3_json_object.side_effect = (
            ClientError(operation_name='GetObject',
                        error_response=s3_client_error_response)
        )
        ga_config = GoogleAnalyticsConfig(GA_CONFIG, '', IMPORTED_TIMESTAMP_FIELD_NAME)
        default_initial_state_timestamp_as_string = (
            "2020-01-01 00:00:00"
        )
        stored_state = get_stored_state(
            ga_config,
            default_initial_state_timestamp_as_string
        )

        assert stored_state == (
            default_initial_state_timestamp_as_string
        )

    def test_should_get_state_from_file_in_s3_bucket(
            self, mock_download_s3_json_object
    ):
        ejp_config = GoogleAnalyticsConfig(GA_CONFIG, '', IMPORTED_TIMESTAMP_FIELD_NAME)
        default_initial_state_timestamp_as_string = ''
        last_stored_modified_timestamp = "2018-01-01 00:00:00"
        mock_download_s3_json_object.return_value = (
            last_stored_modified_timestamp
        )
        stored_state = get_stored_state(
            ejp_config,
            default_initial_state_timestamp_as_string
        )
        assert stored_state == last_stored_modified_timestamp
