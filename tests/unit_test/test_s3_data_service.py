"""
test for s3_data_service
"""
from unittest.mock import patch
import pytest
import boto3
from data_pipeline.utils.cloud_data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
)


@pytest.fixture(name="mock_download_yaml")
def _download_yaml():
    with patch.object(boto3, "client") as mock:
        test_data = UnitTestData()
        mock.return_value.get_object.return_value = (
            test_data.get_source_yaml_s3_response()
        )
        yield mock


@pytest.fixture(name="mock_download_string")
def _download_string():
    with patch.object(boto3, "client") as mock:
        test_data = UnitTestData()
        mock.return_value.get_object.return_value = (
            test_data.get_source_string_s3_response()
        )
        yield mock


def test_download_yaml_as_json_file(mock_download_yaml):
    """
    :param mock_download_yaml:
    :return:
    """
    test_data = UnitTestData()
    json_resp = download_s3_yaml_object_as_json(
        test_data.source_bucket, test_data.source_object
    )
    mock_download_yaml.assert_called_with("s3")
    mock_download_yaml.return_value.get_object.assert_called_with(
        Bucket=test_data.source_bucket, Key=test_data.source_object
    )
    assert json_resp == test_data.expected_yaml_to_json_value


def test_download_string_file(mock_download_string):
    """
    :param mock_download_yaml:
    :return:
    """
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
    """
    test class data
    """

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
        """
        :return:
        """
        response = dict()
        response["Body"] = self.source_yaml
        return response

    def get_source_string_s3_response(self):
        """
        :return:
        """
        response = dict()
        response["Body"] = self.source_sample_string
        return response
