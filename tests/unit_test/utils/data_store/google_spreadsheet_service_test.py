from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.data_store import (
    google_spreadsheet_service as google_spreadsheet_service_module
)
from data_pipeline.utils.data_store.google_spreadsheet_service import (
    get_spreadsheet_modified_timestamp_as_string
)


SPREADSHEET_ID = 'spreadsheet_id_1'


@pytest.fixture(name='get_credentials_mock', autouse=True)
def _get_credentials_mock() -> Iterator[MagicMock]:
    with patch.object(google_spreadsheet_service_module, 'get_credentials') as mock:
        yield mock


@pytest.fixture(name='credentials_mock')
def _credentials_mock(get_credentials_mock: MagicMock) -> MagicMock:
    return get_credentials_mock.return_value


class TestGetSpreadsheetModifiedTimestamp:
    def test_should_request_credentials_with_google_drive_scope(
        self,
        get_credentials_mock: MagicMock
    ):
        get_spreadsheet_modified_timestamp_as_string(SPREADSHEET_ID)
        get_credentials_mock.assert_called_once_with(
            google_spreadsheet_service_module.READ_MODIFIED_TIME_SCOPES
        )

    def test_should_get_drive_service(
        self,
        discovery_build_mock: MagicMock,
        credentials_mock: MagicMock
    ):
        get_spreadsheet_modified_timestamp_as_string(SPREADSHEET_ID)
        discovery_build_mock.assert_called_once_with(
            'drive',
            'v3',
            credentials=credentials_mock
        )

    def test_should_request_modified_time(
        self,
        discovery_build_mock: MagicMock
    ):
        get_spreadsheet_modified_timestamp_as_string(SPREADSHEET_ID)
        discovery_build_mock.return_value.files().get.assert_called_once_with(
            fileId=SPREADSHEET_ID,
            fields='modifiedTime'
        )

    def test_should_return_modified_time(
        self,
        discovery_build_mock: MagicMock
    ):
        discovery_build_mock.return_value.files().get.return_value.execute.return_value = {
            'modifiedTime': '2001-02-03T04:05:06.123Z'
        }
        assert get_spreadsheet_modified_timestamp_as_string(SPREADSHEET_ID) == (
            '2001-02-03T04:05:06.123Z'
        )
