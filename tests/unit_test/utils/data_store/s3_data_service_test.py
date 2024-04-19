import logging
from typing import IO, Iterator
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.compat import six
from botocore.response import StreamingBody

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
    s3_open_binary_read_with_temp_file,
)


LOGGER = logging.getLogger(__name__)

BINARY_DATA_1 = b'binary data 1'


@pytest.fixture(name="mock_s3_client_function", autouse=True)
def _mock_s3_client_function() -> Iterator[MagicMock]:
    with patch.object(boto3, "client") as mock:
        yield mock


@pytest.fixture(name="mock_s3_client", autouse=True)
def _mock_s3_client(mock_s3_client_function: MagicMock) -> MagicMock:
    return mock_s3_client_function.return_value


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
