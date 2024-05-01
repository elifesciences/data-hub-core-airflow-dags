from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.google_analytics import etl_state as etl_state_module
from data_pipeline.google_analytics.etl_state import (
    get_stored_state,
)
from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig


GA_CONFIG = {
    'pipelineID': 'pipeline_id',
    'dataset': 'dataset',
    'table': 'table',
    'viewId': 'viewId',
    'dimensions': ['dimension'],
    'metrics': ['metrics'],
    'recordAnnotations': [{
        'recordAnnotationFieldName': 'recordAnnotationFieldName',
        'recordAnnotationValue': 'recordAnnotationValue'
    }],
    'defaultStartDate': '2001-01-01',
    'stateFile': {
        'bucketName': 'bucket_1',
        'objectName': 'object_1'
    }
}


IMPORTED_TIMESTAMP_FIELD_NAME = 'imported_timestamp'


@pytest.fixture(name="download_s3_object_as_string_or_file_not_found_error_mock")
def _download_s3_object_as_string_or_file_not_found_error_mock() -> Iterator[MagicMock]:
    with patch.object(
        etl_state_module,
        'download_s3_object_as_string_or_file_not_found_error'
    ) as mock:
        yield mock


class TestGetStoredGAProcessingState:
    # pylint: disable=unused-argument
    def test_get_state_no_state_file_in_bucket(
            self,
            download_s3_object_as_string_or_file_not_found_error_mock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.side_effect = (
            FileNotFoundError()
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
            self,
            download_s3_object_as_string_or_file_not_found_error_mock
    ):
        ejp_config = GoogleAnalyticsConfig(GA_CONFIG, '', IMPORTED_TIMESTAMP_FIELD_NAME)
        default_initial_state_timestamp_as_string = ''
        last_stored_modified_timestamp = "2018-01-01 00:00:00"
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            last_stored_modified_timestamp
        )
        stored_state = get_stored_state(
            ejp_config,
            default_initial_state_timestamp_as_string
        )
        assert stored_state == last_stored_modified_timestamp
