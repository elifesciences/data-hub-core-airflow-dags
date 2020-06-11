from datetime import datetime
from unittest.mock import patch
import pytest

from data_pipeline.generic_web_api import (
    generic_web_api_data_etl as generic_web_api_data_etl_module
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    upload_latest_timestamp_as_pipeline_state,
    get_items_list,
    get_next_cursor_from_data
)
from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.generic_web_api.transform_data import ModuleConstant


@pytest.fixture(name='mock_upload_s3_object')
def _upload_s3_object():
    with patch.object(
            generic_web_api_data_etl_module, 'upload_s3_object'
    ) as mock:
        yield mock


WEB_API_CONFIG = {
    'stateFile': {
        'bucketName': '{ENV}-bucket',
        'objectName': '{ENV}/object'
    },
    'dataUrl': {
        'urlExcludingConfigurableParameters':
            'urlExcludingConfigurableParameters'
    }
}
DEP_ENV = 'test'


def get_data_config(
        conf_dict=None,
        dep_env=DEP_ENV
):
    if conf_dict is None:
        conf_dict = WEB_API_CONFIG
    data_config = WebApiConfig(
        conf_dict, '',
        deployment_env=dep_env
    )
    return data_config


class TestUploadLatestTimestampState:
    def test_should_write_latest_date_as_string_to_state_file(
            self,
            mock_upload_s3_object
    ):
        latest_timestamp_string = '2020-01-01 01:01:01+0100'
        latest_timestamp = datetime.strptime(
            latest_timestamp_string,
            ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
        )
        data_config = get_data_config(WEB_API_CONFIG)
        upload_latest_timestamp_as_pipeline_state(
            data_config, latest_timestamp
        )
        state_file_bucket = WEB_API_CONFIG.get(
            'stateFile'
        ).get('bucketName').replace('{ENV}', DEP_ENV)
        state_file_name_key = WEB_API_CONFIG.get(
            'stateFile'
        ).get('objectName').replace('{ENV}', DEP_ENV)
        mock_upload_s3_object.assert_called_with(
            bucket=state_file_bucket,
            object_key=state_file_name_key,
            data_object=latest_timestamp_string,
        )


class TestGetItemList:
    def test_should_return_all_data_when_data_is_a_list(
            self
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        data = [['first'], ['second'], ['third']]
        expected_time_list = get_items_list(
            data,
            data_config,
        )
        assert data == expected_time_list

    def test_should_return_all_data_when_no_data_path_key(
            self
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'key_1': ['first', 'second', 'third']}
        expected_time_list = get_items_list(
            data,
            data_config,
        )
        assert data == expected_time_list

    def test_should_get_data_when_path_keys_are_all_dict_keys_in_data(
            self
    ):
        path_keys = ['data', 'values']
        conf_dict = {
            ** WEB_API_CONFIG,
            'response': {
                'itemsKeyFromResponseRoot': path_keys
            }
        }
        data_config = get_data_config(conf_dict)
        expected_response = ['first', 'second', 'third']
        data = {path_keys[0]: {path_keys[1]: expected_response}}

        actual_response = get_items_list(
            data,
            data_config,
        )
        assert expected_response == actual_response

    def test_should_get_data_when_path_keys_has_keys_of_dict_in_list_of_dict(
            self
    ):
        path_keys = ['data', 'values']
        conf_dict = {
            ** WEB_API_CONFIG,
            'response': {
                'itemsKeyFromResponseRoot': path_keys
            }
        }
        data_config = get_data_config(conf_dict)
        expected_response = ['first', 'second', 'third']
        data = {
            path_keys[0]:
                [
                    {path_keys[1]: expected_response[0]},
                    {path_keys[1]: expected_response[1]},
                    {path_keys[1]: expected_response[2]}
                ]
        }

        actual_response = get_items_list(
            data,
            data_config,
        )
        assert expected_response == actual_response


class TestNextCursor:
    def test_should_be_none_when_cursor_is_not_in_config(self):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'key': 'val', 'values': []}
        assert not get_next_cursor_from_data(data, data_config)

    def test_should_be_none_when_not_in_data(self):
        cursor_path = ['cursor_key1', 'cursor_key2']
        cursor_val = 'cursor_value'

        conf_dict = {
            **WEB_API_CONFIG,
            'response': {
                'nextPageCursorKeyFromResponseRoot': cursor_path
            }
        }
        conf_dict['dataUrl']['configurableParameters'] = {
            'nextPageCursorParameterName': 'cursor'
        }

        data_config = get_data_config(conf_dict)
        data = {
            cursor_path[0]: {cursor_path[1]: cursor_val},
            'values': []
        }
        assert get_next_cursor_from_data(data, data_config) == cursor_val
