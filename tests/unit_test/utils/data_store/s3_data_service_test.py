from unittest.mock import patch

import boto3
import pytest
from botocore.compat import six
from botocore.response import StreamingBody

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
)


@pytest.fixture(name="mock_download_yaml")
def _download_yaml():
    with patch.object(boto3, "client") as mock:
        test_data = UnitTestData()
        data = test_data.source_yaml
        body = six.BytesIO(data.encode())
        streaming_body = StreamingBody(body, len(data.encode()))
        resp = {"Body": streaming_body}
        mock.return_value.get_object.return_value = (
            resp
        )
        yield mock


@pytest.fixture(name="mock_download_string")
def _download_string():
    with patch.object(boto3, "client") as mock:
        test_data = UnitTestData()
        data = test_data.source_sample_string
        body = six.BytesIO(data.encode())
        streaming_body = StreamingBody(body, len(data.encode()))
        resp = {"Body": streaming_body}
        mock.return_value.get_object.return_value = (
            resp
        )
        yield mock


def test_should_download_yaml_as_json_file(mock_download_yaml):
    test_data = UnitTestData()
    json_resp = download_s3_yaml_object_as_json(
        test_data.source_bucket, test_data.source_object
    )
    mock_download_yaml.assert_called_with("s3")
    mock_download_yaml.return_value.get_object.assert_called_with(
        Bucket=test_data.source_bucket, Key=test_data.source_object
    )
    assert json_resp == test_data.expected_yaml_to_json_value


def test_should_download_string_file(mock_download_string):
    test_data = UnitTestData()
    resp = download_s3_yaml_object_as_json(
        test_data.source_bucket, test_data.source_object
    )
    mock_download_string.assert_called_with("s3")
    mock_download_string.return_value.get_object.assert_called_with(
        Bucket=test_data.source_bucket, Key=test_data.source_object
    )
    assert resp == test_data.source_sample_string


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
