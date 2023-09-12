from datetime import datetime, timedelta
from typing import Sequence
from unittest.mock import MagicMock, patch, ANY
import pytest

from data_pipeline.generic_web_api import (
    generic_web_api_data_etl as generic_web_api_data_etl_module
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    upload_latest_timestamp_as_pipeline_state,
    get_items_list,
    get_next_cursor_from_data,
    get_next_page_number,
    get_next_offset,
    generic_web_api_data_etl
)
from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.generic_web_api.module_constants import ModuleConstant


@pytest.fixture(name='mock_upload_s3_object')
def _upload_s3_object():
    with patch.object(
            generic_web_api_data_etl_module, 'upload_s3_object'
    ) as mock:
        yield mock


@pytest.fixture(name='process_downloaded_data_mock')
def _process_downloaded_data_mock():
    with patch.object(
            generic_web_api_data_etl_module, 'process_downloaded_data'
    ) as mock:
        yield mock


@pytest.fixture(
    name='get_start_timestamp_from_state_file_or_optional_default_value_mock',
    autouse=True
)
def _get_start_timestamp_from_state_file_or_optional_default_value_mock():
    with patch.object(
        generic_web_api_data_etl_module,
        'get_start_timestamp_from_state_file_or_optional_default_value'
    ) as mock:
        yield mock


@pytest.fixture(name='get_data_single_page_mock', autouse=True)
def _get_data_single_page_mock():
    with patch.object(
            generic_web_api_data_etl_module, 'get_data_single_page'
    ) as mock:
        yield mock


@pytest.fixture(name='load_written_data_to_bq_mock', autouse=True)
def _load_written_data_to_bq_mock():
    with patch.object(
            generic_web_api_data_etl_module, 'load_written_data_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='upload_latest_timestamp_as_pipeline_state_mock', autouse=True)
def _upload_latest_timestamp_as_pipeline_state_mock():
    with patch.object(
            generic_web_api_data_etl_module, 'upload_latest_timestamp_as_pipeline_state'
    ) as mock:
        yield mock


@pytest.fixture(name='get_items_list_mock')
def _get_items_list_mock():
    with patch.object(
            generic_web_api_data_etl_module, 'get_items_list'
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
    data_config = WebApiConfig.from_dict(
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
        actual_response = get_items_list(
            data,
            data_config,
        )
        assert actual_response == data

    def test_should_return_all_data_when_no_data_path_key(
            self
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'key_1': ['first', 'second', 'third']}
        actual_response = get_items_list(
            data,
            data_config,
        )
        assert actual_response == [data]

    def test_should_return_key_list_even_the_list_is_empty(
            self
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'key_1': []}
        actual_response = get_items_list(
            data,
            data_config,
        )
        assert actual_response == [data]

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
        assert actual_response == expected_response

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
        assert actual_response == expected_response

    def test_should_raise_exception_if_key_not_found(
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
        data = {'error': 'error message'}

        with pytest.raises(ValueError):
            get_items_list(data, data_config)


def _get_web_api_config_with_cursor_path(cursor_path: Sequence[str]) -> WebApiConfig:
    conf_dict: dict = {
        **WEB_API_CONFIG,
        'response': {
            'nextPageCursorKeyFromResponseRoot': cursor_path
        },
        'dataUrl': {
            'configurableParameters': {
                'nextPageCursorParameterName': 'cursor'
            }
        }
    }
    return get_data_config(conf_dict)


class TestNextCursor:
    def test_should_be_none_when_cursor_parameter_is_not_in_config(self):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'key': 'val', 'values': []}
        assert get_next_cursor_from_data(data, data_config, previous_cursor=None) is None

    def test_should_get_cursor_value_when_in_data_and_configured_without_previous_cursor(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data = {'cursor_key1': 'cursor1'}
        actual_next_cursor = get_next_cursor_from_data(
            data, data_config, previous_cursor=None
        )
        assert actual_next_cursor == 'cursor1'

    def test_should_get_cursor_value_when_in_data_and_configured_with_previous_cursor(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data = {'cursor_key1': 'cursor1'}
        actual_next_cursor = get_next_cursor_from_data(
            data, data_config, previous_cursor='cursor0'
        )
        assert actual_next_cursor == 'cursor1'

    def test_should_ignore_get_cursor_value_when_matching_previous_cursor(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data = {'cursor_key1': 'cursor1'}
        actual_next_cursor = get_next_cursor_from_data(
            data, data_config, previous_cursor='cursor1'
        )
        assert actual_next_cursor is None

    def test_should_be_none_when_configured_but_not_in_data(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data = {}
        actual_next_cursor = get_next_cursor_from_data(
            data, data_config, previous_cursor=None
        )
        assert actual_next_cursor is None


class TestNextPage:

    def test_should_be_none_when_page_parameter_is_not_in_config(self):
        data_config = get_data_config(WEB_API_CONFIG)
        current_item_count = 10
        current_page = 0
        assert not get_next_page_number(
            current_item_count, current_page, data_config
        )

    def test_should_be_none_when_item_count_is_less_than_page_size(self):

        conf_dict = {
            **WEB_API_CONFIG,
        }
        conf_dict['dataUrl']['configurableParameters'] = {
            'pageSizeParameterName': 'per-page',
            'defaultPageSize': 100,
            'pageParameterName': 'page'
        }

        data_config = get_data_config(conf_dict)
        current_item_count = 10
        current_page = 0
        assert not get_next_page_number(
            current_item_count, current_page, data_config
        )

    def test_should_increase_page_by_1_if_item_count_is_equal_to_page_size(
            self
    ):

        conf_dict = {
            **WEB_API_CONFIG,
        }
        conf_dict['dataUrl']['configurableParameters'] = {
            'pageSizeParameterName': 'per-page',
            'defaultPageSize': 100,
            'pageParameterName': 'page'
        }

        data_config = get_data_config(conf_dict)
        current_item_count = 100
        current_page = 0
        assert (
            get_next_page_number(
                current_item_count, current_page, data_config
            ) ==
            (current_page + 1)
        )


class TestNextOffset:

    def test_should_be_none_when_offset_parameter_is_not_in_config(self):
        data_config = get_data_config(WEB_API_CONFIG)
        current_item_count = 10
        current_offset = 0
        assert not get_next_offset(
            current_item_count, current_offset, data_config
        )

    def test_should_be_none_if_item_count_is_less_than_page_size(self):

        conf_dict = {
            **WEB_API_CONFIG,
        }
        conf_dict['dataUrl']['configurableParameters'] = {
            'offsetParameterName': 'offset',
            'pageSizeParameterName': 'per-page',
            'defaultPageSize': 100,
        }
        data_config = get_data_config(conf_dict)
        current_offset = 0
        current_item_count = 10
        assert not get_next_offset(
            current_item_count, current_offset, data_config
        )

    def test_should_increase_offset_by_pg_size_if_item_count_equals_page_size(
            self
    ):
        conf_dict = {
            **WEB_API_CONFIG,
        }
        conf_dict['dataUrl']['configurableParameters'] = {
            'offsetParameterName': 'offset',
            'pageSizeParameterName': 'per-page',
            'defaultPageSize': 100,
        }

        data_config = get_data_config(conf_dict)
        current_item_count = 100
        current_offset = 0
        assert (
            get_next_offset(current_item_count, current_offset, data_config) ==
            (current_offset + current_item_count)
        )


class TestGenericWebApiDataEtl:
    def test_should_pass_null_value_removed_item_list_to_process_downloaded_data(
        self,
        get_items_list_mock: MagicMock,
        process_downloaded_data_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        get_items_list_mock.return_value = [{'key_1': ['value1'], 'key_2': []}]
        generic_web_api_data_etl(data_config)
        process_downloaded_data_mock.assert_called_with(
            data_config=data_config,
            record_list=[{'key_1': ['value1']}],
            data_etl_timestamp=ANY,
            file_location=ANY,
            prev_page_latest_timestamp=ANY
        )

    def test_should_update_state_file(
        self,
        get_items_list_mock: MagicMock,
        process_downloaded_data_mock: MagicMock,
        upload_latest_timestamp_as_pipeline_state_mock: MagicMock
    ):
        timestamp_string = '2020-01-01T01:01:01+00:00'
        timestamp = datetime.fromisoformat(timestamp_string)
        data_config = get_data_config(WEB_API_CONFIG)
        get_items_list_mock.return_value = [{'timestamp': timestamp}]
        process_downloaded_data_mock.return_value = timestamp
        generic_web_api_data_etl(data_config)
        upload_latest_timestamp_as_pipeline_state_mock.assert_called_with(
            data_config,
            process_downloaded_data_mock.return_value
        )

    def test_should_not_update_state_with_empty_list_in_response(
        self,
        get_data_single_page_mock: MagicMock,
        upload_latest_timestamp_as_pipeline_state_mock: MagicMock
    ):
        conf_dict: dict = {
            ** WEB_API_CONFIG,
            'response': {
                'itemsKeyFromResponseRoot': ['rows']
            }
        }
        data_config = get_data_config(conf_dict)
        get_data_single_page_mock.return_value = {'rows': []}
        generic_web_api_data_etl(data_config)
        upload_latest_timestamp_as_pipeline_state_mock.assert_not_called()

    def test_should_retrieve_data_in_date_range_batches(
        self,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock,
        get_data_single_page_mock: MagicMock
    ):
        timestamp_string_1 = '2020-01-01+00:00'
        timestamp_string_2 = '2020-01-02+00:00'
        initial_timestamp = datetime.fromisoformat(timestamp_string_1)
        batch_size_in_days = 10
        end_timestamp = datetime.fromisoformat('2020-01-20+00:00')
        expected_from_and_until_date_list = [
            (
                initial_timestamp,
                initial_timestamp + timedelta(days=batch_size_in_days)
            ),
            (
                initial_timestamp + timedelta(days=batch_size_in_days),
                initial_timestamp + timedelta(days=2 * batch_size_in_days)
            )
        ]
        data_config = (
            get_data_config(WEB_API_CONFIG)
            ._replace(
                item_timestamp_key_path_from_item_root=['timestamp'],
                start_to_end_date_diff_in_days=batch_size_in_days,
                default_start_date=timestamp_string_1
            )
        )
        get_start_timestamp_from_state_file_or_optional_default_value_mock.return_value = (
            initial_timestamp
        )
        item_list = [{'timestamp': timestamp_string_2}]
        get_data_single_page_mock.return_value = item_list
        generic_web_api_data_etl(data_config, end_timestamp=end_timestamp)
        actual_from_and_until_date_list = [
            (call_args.kwargs['from_date'], call_args.kwargs['until_date'])
            for call_args in get_data_single_page_mock.call_args_list
        ]
        assert actual_from_and_until_date_list == expected_from_and_until_date_list

    def test_should_pass_none_from_and_until_dates_if_not_configured(
        self,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock,
        get_data_single_page_mock: MagicMock
    ):
        expected_from_and_until_date_list = [(None, None)]
        data_config = get_data_config(WEB_API_CONFIG)
        get_start_timestamp_from_state_file_or_optional_default_value_mock.return_value = None
        item_list = [{'key': 'value'}]
        get_data_single_page_mock.return_value = item_list
        generic_web_api_data_etl(data_config)
        actual_from_and_until_date_list = [
            (call_args.kwargs['from_date'], call_args.kwargs['until_date'])
            for call_args in get_data_single_page_mock.call_args_list
        ]
        assert actual_from_and_until_date_list == expected_from_and_until_date_list
