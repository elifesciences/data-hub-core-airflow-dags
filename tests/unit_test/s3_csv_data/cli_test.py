
from datetime import datetime
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.data_store.s3_data_service import (
    FileMetadata,
    FileMetadataWithObjectPattern
)

import data_pipeline.s3_csv_data.cli as cli_module
from data_pipeline.s3_csv_data.s3_csv_config import S3BaseCsvConfig


TIMESTAMP_STRING_1 = '2020-01-01T00:00:00+00:00'
TIMESTAMP_STRING_2 = '2020-01-02T00:00:00+00:00'

TIMESTAMP_1 = datetime.fromisoformat(TIMESTAMP_STRING_1)
TIMESTAMP_2 = datetime.fromisoformat(TIMESTAMP_STRING_2)

S3_BUCKET_NAME_1 = 's3_bucket_name_1'

OBJECT_KEY_1 = 'object_key_1'

OBJECT_PATTERN_1 = 'object_pattern_1*'


FILE_METADATA_1 = FileMetadata(
    bucket=S3_BUCKET_NAME_1,
    name=OBJECT_KEY_1,
    last_modified=TIMESTAMP_1
)


CSV_CONFIG_DICT_1 = {
    'dataPipelineId': 'data_pipeline_id_1',
    'importedTimestampFieldName': 'imported_timestamp',
    'objectKeyPattern': [OBJECT_PATTERN_1],
    'stateFile': {
        'bucketName': '{ENV}_bucket_name',
        'objectName': '{ENV}_object_prefix_1'
    }
}


@pytest.fixture(name='get_current_timestamp_as_string_mock')
def _get_current_timestamp_as_string_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'get_current_timestamp_as_string') as mock:
        yield mock


@pytest.fixture(name='iter_sorted_new_s3_files_to_process_mock')
def _iter_sorted_new_s3_files_to_process_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'iter_sorted_new_s3_files_to_process') as mock:
        yield mock


@pytest.fixture(name='transform_load_data_mock')
def _transform_load_data_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'transform_load_data') as mock:
        yield mock


@pytest.fixture(name='update_object_latest_dates_mock')
def _update_object_latest_dates_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'update_object_latest_dates') as mock:
        yield mock


@pytest.fixture(name='get_stored_state_mock')
def _get_stored_state_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'get_stored_state') as mock:
        yield mock


def get_s3_csv_config(csv_config_dict: dict):
    gcp_project = ''
    deployment_env = ''
    return S3BaseCsvConfig(
        csv_config_dict,
        gcp_project,
        deployment_env
    )


class TestEtlNewCsvFiles:
    def test_should_call_iter_sorted_new_s3_files_to_process(
        self,
        iter_sorted_new_s3_files_to_process_mock: MagicMock,
        get_stored_state_mock: MagicMock
    ):
        get_stored_state_mock.return_value = {OBJECT_PATTERN_1: TIMESTAMP_1}
        cli_module.etl_new_csv_files(
            data_config=get_s3_csv_config({
                **CSV_CONFIG_DICT_1,
                'bucketName': S3_BUCKET_NAME_1,
                'objectKeyPattern': [OBJECT_PATTERN_1],
            })
        )
        iter_sorted_new_s3_files_to_process_mock.assert_called_with(
            obj_pattern_with_latest_dates={OBJECT_PATTERN_1: TIMESTAMP_1},
            s3_bucket_name=S3_BUCKET_NAME_1
        )

    def test_should_call_transform_load_data(
        self,
        iter_sorted_new_s3_files_to_process_mock: MagicMock,
        transform_load_data_mock: MagicMock,
        get_current_timestamp_as_string_mock: MagicMock,
        get_stored_state_mock: MagicMock
    ):
        get_stored_state_mock.return_value = {OBJECT_PATTERN_1: TIMESTAMP_1}
        iter_sorted_new_s3_files_to_process_mock.return_value = iter([
            FileMetadataWithObjectPattern(
                file_metadata=FILE_METADATA_1,
                object_key_pattern=OBJECT_PATTERN_1
            )
        ])
        data_config = get_s3_csv_config({
            **CSV_CONFIG_DICT_1,
            'bucketName': S3_BUCKET_NAME_1,
            'objectKeyPattern': [OBJECT_PATTERN_1],
        })
        cli_module.etl_new_csv_files(data_config=data_config)
        transform_load_data_mock.assert_called_with(
            FILE_METADATA_1.name,
            data_config,
            get_current_timestamp_as_string_mock.return_value
        )

    def test_should_call_update_object_latest_dates(
        self,
        iter_sorted_new_s3_files_to_process_mock: MagicMock,
        update_object_latest_dates_mock: MagicMock,
        get_stored_state_mock: MagicMock
    ):
        get_stored_state_mock.return_value = {OBJECT_PATTERN_1: TIMESTAMP_1}
        iter_sorted_new_s3_files_to_process_mock.return_value = iter([
            FileMetadataWithObjectPattern(
                file_metadata=FILE_METADATA_1,
                object_key_pattern=OBJECT_PATTERN_1
            )
        ])
        data_config = get_s3_csv_config({
            **CSV_CONFIG_DICT_1,
            'bucketName': S3_BUCKET_NAME_1,
            'objectKeyPattern': [OBJECT_PATTERN_1],
        })
        update_object_latest_dates_mock.return_value = {}
        cli_module.etl_new_csv_files(data_config=data_config)
        update_object_latest_dates_mock.assert_called_with(
            obj_pattern_with_latest_dates={OBJECT_PATTERN_1: TIMESTAMP_1},
            object_pattern=OBJECT_PATTERN_1,
            file_modified_timestamp=TIMESTAMP_1
        )
