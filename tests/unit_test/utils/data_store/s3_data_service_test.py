from datetime import datetime
import logging
from typing import IO, Iterator
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.compat import six
from botocore.response import StreamingBody

from data_pipeline.utils.data_store.s3_data_service import (
    FileMetadata,
    FileMetadataWithObjectPattern,
    download_s3_yaml_object_as_json,
    iter_sorted_new_s3_files_to_process,
    s3_open_binary_read_with_temp_file,
)

import data_pipeline.utils.data_store.s3_data_service as s3_data_service_module


LOGGER = logging.getLogger(__name__)

BINARY_DATA_1 = b'binary data 1'

TIMESTAMP_STRING_1 = '2020-01-01T00:00:00+00:00'
TIMESTAMP_STRING_2 = '2020-01-02T00:00:00+00:00'

TIMESTAMP_1 = datetime.fromisoformat(TIMESTAMP_STRING_1)
TIMESTAMP_2 = datetime.fromisoformat(TIMESTAMP_STRING_2)

S3_BUCKET_NAME_1 = 's3_bucket_name_1'

OBJECT_PATTERN_1 = 'object_pattern_1*'

OBJECT_KEY_1 = 'object_key_1'
OBJECT_KEY_2 = 'object_key_2'

FILE_METADATA_1 = FileMetadata(
    bucket=S3_BUCKET_NAME_1,
    name=OBJECT_KEY_1,
    last_modified=TIMESTAMP_1
)

FILE_METADATA_2 = FileMetadata(
    bucket=S3_BUCKET_NAME_1,
    name=OBJECT_KEY_2,
    last_modified=TIMESTAMP_2
)


@pytest.fixture(name="mock_s3_client_function", autouse=True)
def _mock_s3_client_function() -> Iterator[MagicMock]:
    with patch.object(boto3, "client") as mock:
        yield mock


@pytest.fixture(name="mock_s3_client", autouse=True)
def _mock_s3_client(mock_s3_client_function: MagicMock) -> MagicMock:
    return mock_s3_client_function.return_value


@pytest.fixture(name='mock_list_objects_with_pattern_and_timestamp', autouse=True)
def _mock_list_objects_with_pattern_and_timestamp() -> Iterator[MagicMock]:
    with patch.object(s3_data_service_module, 'list_objects_with_pattern_and_timestamp') as mock:
        yield mock


def _mock_download_fileobj(Bucket: str, Key: str, Fileobj: IO):  # pylint: disable=invalid-name
    LOGGER.debug('Bucket=%r, Key=%r', Bucket, Key)
    Fileobj.write(BINARY_DATA_1)


class TestS3OpenBinaryReadWithTempFile:
    def test_should_return_a_stream_with_data_from_s3(
        self,
        mock_s3_client: MagicMock
    ):
        LOGGER.debug('mock_s3_client: %r', mock_s3_client)
        mock_s3_client.download_fileobj = _mock_download_fileobj
        with s3_open_binary_read_with_temp_file(
            bucket='bucket_1',
            object_key='object_1'
        ) as data_fp:
            assert data_fp.read() == BINARY_DATA_1


def _get_streaming_body_for_data(data: str) -> StreamingBody:
    body = six.BytesIO(data.encode())
    streaming_body = StreamingBody(body, len(data.encode()))
    get_object_response = {"Body": streaming_body}
    return get_object_response


def test_should_download_yaml_as_json_file(
    mock_s3_client: MagicMock,
    mock_s3_client_function: MagicMock
):
    test_data = UnitTestData()

    mock_s3_client.get_object.return_value = _get_streaming_body_for_data(
        test_data.source_yaml
    )

    json_resp = download_s3_yaml_object_as_json(
        test_data.source_bucket, test_data.source_object
    )
    mock_s3_client_function.assert_called_with("s3")
    mock_s3_client.get_object.assert_called_with(
        Bucket=test_data.source_bucket, Key=test_data.source_object
    )
    assert json_resp == test_data.expected_yaml_to_json_value


def test_should_download_string_file(
    mock_s3_client: MagicMock,
    mock_s3_client_function: MagicMock
):
    test_data = UnitTestData()

    mock_s3_client.get_object.return_value = _get_streaming_body_for_data(
        test_data.source_sample_string
    )

    get_object_response = download_s3_yaml_object_as_json(
        test_data.source_bucket, test_data.source_object
    )
    mock_s3_client_function.assert_called_with("s3")
    mock_s3_client.get_object.assert_called_with(
        Bucket=test_data.source_bucket, Key=test_data.source_object
    )
    assert get_object_response == test_data.source_sample_string


class UnitTestData:
    def __init__(self):
        self.source_bucket = "test_bucket"
        self.source_object = "test_object"
        self.source_yaml = """
            projectName: 'project_name'
            dataset: 'dataset'
            tempObjectDir:
                bucket: 'temp_obj_dir_bucket'
        """
        self.expected_yaml_to_json_value = {
            "projectName": "project_name",
            "dataset": "dataset",
            "tempObjectDir": {"bucket": "temp_obj_dir_bucket"},
        }
        self.source_sample_string = "sample_string"

    def get_source_yaml_s3_response(self):
        response = {}
        response["Body"] = self.source_yaml
        return response

    def get_source_string_s3_response(self):
        response = {}
        response["Body"] = self.source_sample_string
        return response


class TestIterSortedNewS3FilesToProcess:
    def test_should_call_list_objects_with_pattern_and_timestamp(
        self,
        mock_list_objects_with_pattern_and_timestamp: MagicMock,
        mock_s3_client: MagicMock
    ):
        list(iter_sorted_new_s3_files_to_process(
            obj_pattern_with_latest_dates={OBJECT_PATTERN_1: TIMESTAMP_1},
            s3_bucket_name=S3_BUCKET_NAME_1
        ))

        mock_list_objects_with_pattern_and_timestamp.assert_called_with(
            s3_client=mock_s3_client,
            bucket=S3_BUCKET_NAME_1,
            pattern=OBJECT_PATTERN_1,
            latest_timestamp=TIMESTAMP_1
        )

    def test_should_return_file_metadata_with_object_pattern(
        self,
        mock_list_objects_with_pattern_and_timestamp: MagicMock
    ):
        mock_list_objects_with_pattern_and_timestamp.return_value = [
            FILE_METADATA_1
        ]
        assert list(iter_sorted_new_s3_files_to_process(
            obj_pattern_with_latest_dates={OBJECT_PATTERN_1: TIMESTAMP_1},
            s3_bucket_name=S3_BUCKET_NAME_1
        )) == [
            FileMetadataWithObjectPattern(
                file_metadata=FILE_METADATA_1,
                object_key_pattern=OBJECT_PATTERN_1
            )
        ]

    def test_should_return_sorted_file_metadata_with_object_pattern(
        self,
        mock_list_objects_with_pattern_and_timestamp: MagicMock
    ):
        mock_list_objects_with_pattern_and_timestamp.return_value = [
            FILE_METADATA_2,
            FILE_METADATA_1
        ]
        assert list(iter_sorted_new_s3_files_to_process(
            obj_pattern_with_latest_dates={OBJECT_PATTERN_1: TIMESTAMP_1},
            s3_bucket_name=S3_BUCKET_NAME_1
        )) == [
            FileMetadataWithObjectPattern(
                file_metadata=FILE_METADATA_1,
                object_key_pattern=OBJECT_PATTERN_1
            ),
            FileMetadataWithObjectPattern(
                file_metadata=FILE_METADATA_2,
                object_key_pattern=OBJECT_PATTERN_1
            )
        ]
