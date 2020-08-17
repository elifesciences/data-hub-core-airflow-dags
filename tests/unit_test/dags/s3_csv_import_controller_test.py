from unittest.mock import patch
import pytest

from dags import s3_csv_import_controller
from dags.s3_csv_import_controller import (
    trigger_s3_csv_import_pipeline_dag, TARGET_DAG
)


class TestData:
    TEST_DATA_MULTIPLE_S3_CSV_PATTERN_SET = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "stateFile": {
            "defaultBucketName": "{ENV}_bucket_name",
            "defaultSystemGeneratedObjectPrefix": "{ENV}_object_prefix"
        },
        "s3Csv": [
            {
                "dataPipelineId": "data-pipeline-id-1",
                "objectKeyPattern": [
                    "obj_key_pattern_1*",
                    "obj_key_pattern_2*"
                ],
                "stateFile": {
                    "bucketName": "{ENV}_bucket_name",
                    "objectName": "{ENV}_object_prefix_1"
                }
            },
            {
                "dataPipelineId": "data-pipeline-id-2",
                "objectKeyPattern": [
                    "obj_key_pattern_3*",
                    "obj_key_pattern_4*"
                ],
                "stateFile": {
                    "bucketName": "{ENV}_bucket_name",
                    "objectName": "{ENV}_object_prefix_2"
                }
            }
        ]
    }
    TEST_DATA_SINGLE_S3_CSV_PATTERN_SET = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "s3Csv": [
            {
                "dataPipelineId": "data-pipeline-id-1",
                "objectKeyPattern": [
                    "obj_key_pattern_1*",
                    "obj_key_pattern_2*"
                ],
                "stateFile": {
                    "bucketName": "{ENV}_bucket_name",
                    "objectName": "{ENV}_object_prefix_1"
                }
            }
        ]
    }

    def __init__(self):
        self.pattern_set_count = len(
            TestData.TEST_DATA_MULTIPLE_S3_CSV_PATTERN_SET.get(
                "s3Csv"
            )
        )


@pytest.fixture(name="mock_get_yaml_file_as_dict")
def _get_yaml_file_as_dict():
    with patch.object(s3_csv_import_controller,
                      "get_yaml_file_as_dict") as mock:
        yield mock


@pytest.fixture(name="mock_simple_trigger_dag")
def _simple_dag_trigger():
    with patch.object(s3_csv_import_controller,
                      "simple_trigger_dag") as mock:
        yield mock


def test_should_call_trigger_dag_function_n_times(
        mock_simple_trigger_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_MULTIPLE_S3_CSV_PATTERN_SET
    )
    test_data = TestData()
    trigger_s3_csv_import_pipeline_dag()
    assert mock_simple_trigger_dag.call_count == test_data.pattern_set_count


def test_should_call_trigger_dag_function_with_parameter(
        mock_simple_trigger_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_SINGLE_S3_CSV_PATTERN_SET
    )
    single_s3_csv_pattern_set_config = {
        **(
            TestData.TEST_DATA_SINGLE_S3_CSV_PATTERN_SET.get(
                "s3Csv"
            )[0]
        ),
        'gcpProjectName':
            TestData.TEST_DATA_SINGLE_S3_CSV_PATTERN_SET.get(
                "gcpProjectName"
            ),
        'importedTimestampFieldName':
            TestData.TEST_DATA_SINGLE_S3_CSV_PATTERN_SET.get(
                "importedTimestampFieldName"
            )
    }

    trigger_s3_csv_import_pipeline_dag()
    mock_simple_trigger_dag.assert_called_with(
        dag_id=TARGET_DAG, conf=single_s3_csv_pattern_set_config
    )
