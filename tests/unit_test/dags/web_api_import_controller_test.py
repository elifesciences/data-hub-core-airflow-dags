from unittest.mock import patch
import pytest

from data_pipeline.utils.pipeline_config import ConfigKeys

from dags import web_api_import_controller
from dags.web_api_import_controller import (
    trigger_web_api_data_import_pipeline_dag,
    TARGET_DAG_ID
)


@pytest.fixture(name="mock_get_yaml_file_as_dict")
def _get_yaml_file_as_dict():
    with patch.object(web_api_import_controller,
                      "get_yaml_file_as_dict") as mock:
        yield mock


@pytest.fixture(name="mock_trigger_data_pipeline_dag")
def _trigger_data_pipeline_dag():
    with patch.object(web_api_import_controller,
                      "trigger_data_pipeline_dag") as mock:
        yield mock


class TestData:
    TEST_WEB_API_CONFIG_ID_1 = "test1"
    TEST_WEB_API_CONFIG_ID_2 = "test2"

    TEST_DATA_WEB_API_CONFIG_1 = {
        ConfigKeys.DATA_PIPELINE_CONFIG_ID: TEST_WEB_API_CONFIG_ID_1,
        "datataUrl": {
            "urlExcludingConfigurableParameters": "url-1",
            "configurableParameters":
                {
                    "pageSizeParameterName": "page-size",
                }
        }
    }

    TEST_DATA_WEB_API_CONFIG_2 = {
        ConfigKeys.DATA_PIPELINE_CONFIG_ID: TEST_WEB_API_CONFIG_ID_2,
        "datataUrl": {
            "urlExcludingConfigurableParameters": "url-2",
            "configurableParameters":
                {
                    "pageSizeParameterName": "page-size",
                }
        }
    }

    TEST_DATA_MULTI_WEB_API = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "webApi": [TEST_DATA_WEB_API_CONFIG_1, TEST_DATA_WEB_API_CONFIG_2]
    }
    TEST_DATA_SINGLE_WEB_API = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "webApi": [TEST_DATA_WEB_API_CONFIG_1]
    }

    def __init__(self):
        self.web_api_count = len(
            TestData.TEST_DATA_MULTI_WEB_API.get(
                "webApi"
            )
        )


def test_should_call_trigger_dag_function_n_times(
        mock_trigger_data_pipeline_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_MULTI_WEB_API
    )
    test_data = TestData()
    trigger_web_api_data_import_pipeline_dag()
    assert mock_trigger_data_pipeline_dag.call_count == test_data.web_api_count


def test_should_call_trigger_dag_function_with_parameter(
        mock_trigger_data_pipeline_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_SINGLE_WEB_API
    )
    single_web_api_config = {
        **(
            TestData.TEST_DATA_SINGLE_WEB_API.get(
                "webApi"
            )[0]
        ),
        'gcpProjectName':
            TestData.TEST_DATA_SINGLE_WEB_API.get(
                "gcpProjectName"
            ),
        'importedTimestampFieldName':
            TestData.TEST_DATA_SINGLE_WEB_API.get(
                "importedTimestampFieldName"
            )
    }

    trigger_web_api_data_import_pipeline_dag()
    mock_trigger_data_pipeline_dag.assert_called_with(
        dag_id=TARGET_DAG_ID, conf=single_web_api_config
    )
