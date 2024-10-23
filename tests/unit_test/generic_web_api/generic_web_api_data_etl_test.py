# pylint: disable=too-many-lines
import dataclasses
from datetime import datetime, timedelta
import functools
import itertools
import logging
from typing import Iterable, Iterator, List, Sequence, cast
from unittest.mock import ANY, MagicMock, patch
import pytest

from data_pipeline.generic_web_api import (
    generic_web_api_data_etl as generic_web_api_data_etl_module
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    WebApiPageResponse,
    get_data_single_page_response,
    get_initial_dynamic_request_parameters,
    get_next_dynamic_request_parameters_for_page_data,
    get_optional_total_count,
    get_start_timestamp_from_state_file_or_optional_default_value,
    iter_processed_web_api_data_etl_batch_data,
    process_web_api_data_etl_batch,
    process_web_api_data_etl_batch_with_batch_source_value,
    process_web_api_data_etl_batch_with_batch_source_value_and_date_range,
    upload_latest_timestamp_as_pipeline_state,
    get_items_list,
    get_next_cursor_from_data,
    get_next_page_number,
    get_next_offset,
    generic_web_api_data_etl
)
from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig, WebApiResponseConfig
from data_pipeline.generic_web_api.module_constants import ModuleConstant
from data_pipeline.generic_web_api.generic_web_api_config_typing import (
    WebApiConfigDict
)
from data_pipeline.generic_web_api.request_builder import WebApiDynamicRequestParameters
from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict
from data_pipeline.utils.pipeline_file_io import iter_write_jsonl_to_file

from tests.unit_test.generic_web_api.test_data import DEP_ENV, get_data_config


LOGGER = logging.getLogger(__name__)


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}

REQUEST_PROVENANCE_1 = {
    'api_url': 'url-1'
}

TIMESTAMP_STRING_1 = '2020-01-01T00:00:00+00:00'
TIMESTAMP_STRING_2 = '2020-01-02T00:00:00+00:00'

TIMESTAMP_1 = datetime.fromisoformat(TIMESTAMP_STRING_1)
TIMESTAMP_2 = datetime.fromisoformat(TIMESTAMP_STRING_2)

SOURCE_VALUE_1 = {'source': 'value 1'}
SOURCE_VALUE_2 = {'source': 'value 2'}

PLACEHOLDER_VALUES_1 = {'placeholder': 'buddy1'}


TEST_IMPORTED_TIMESTAMP_FIELD_NAME = 'imported_timestamp_1'

STATE_FILE_1: StateFileConfigDict = {
    'bucketName': '{ENV}-bucket',
    'objectName': '{ENV}/object'
}

WEB_API_CONFIG: WebApiConfigDict = {
    'dataPipelineId': 'pipeline_1',
    'gcpProjectName': 'project_1',
    'importedTimestampFieldName': TEST_IMPORTED_TIMESTAMP_FIELD_NAME,
    'dataset': 'dataset_1',
    'table': 'table_1',
    'dataUrl': {
        'urlExcludingConfigurableParameters':
            'urlExcludingConfigurableParameters'
    }
}

WEB_API_WITH_TIMESTAMP_FIELD_CONFIG_DICT = cast(WebApiConfigDict, {
    **WEB_API_CONFIG,
    'response': {
        'recordTimestamp': {
            'itemTimestampKeyFromItemRoot': ['timestamp']
        }
    }
})


@pytest.fixture(name='get_current_timestamp_mock')
def _get_current_timestamp_mock() -> Iterator[MagicMock]:
    with patch.object(generic_web_api_data_etl_module, 'get_current_timestamp') as mock:
        yield mock


@pytest.fixture(name='requests_response_mock')
def _requests_response_mock() -> MagicMock:
    response_mock = MagicMock(name='response_mock')
    response_mock.content = b'{}'
    return response_mock


@pytest.fixture(name='requests_session_mock')
def _requests_session_mock() -> MagicMock:
    requests_session_mock = MagicMock(name='requests_session_mock')
    return requests_session_mock


@pytest.fixture(name='requests_retry_session_mock', autouse=True)
def _requests_retry_session_mock(requests_session_mock: MagicMock) -> Iterator[MagicMock]:
    with patch.object(
        generic_web_api_data_etl_module,
        'requests_retry_session_for_config'
    ) as mock:
        mock.return_value.__enter__.return_value = requests_session_mock
        yield mock


@pytest.fixture(name='get_response_and_provenance_from_api_mock', autouse=True)
def _get_response_and_provenance_from_api_mock(
    requests_response_mock: MagicMock
) -> Iterator[MagicMock]:
    with patch.object(
        generic_web_api_data_etl_module,
        'get_response_and_provenance_from_api'
    ) as mock:
        mock.return_value = (requests_response_mock, MagicMock(name='provenance'))
        yield mock


@pytest.fixture(name='download_s3_object_as_string_or_file_not_found_error_mock')
def _download_s3_object_as_string_or_file_not_found_error_mock():
    with patch.object(
        generic_web_api_data_etl_module, 'download_s3_object_as_string_or_file_not_found_error'
    ) as mock:
        yield mock


@pytest.fixture(name='mock_upload_s3_object')
def _upload_s3_object():
    with patch.object(
        generic_web_api_data_etl_module, 'upload_s3_object'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_processed_record_for_api_item_list_response_mock')
def _iter_processed_record_for_api_item_list_response_mock():
    with patch.object(
        generic_web_api_data_etl_module, 'iter_processed_record_for_api_item_list_response'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_write_jsonl_to_file_mock_json_lists')
def _iter_write_jsonl_to_file_mock_json_lists() -> List[List[dict]]:
    return []


def _iter_write_jsonl_to_file_side_effect(*args, json_lists: List[List[dict]], **kwargs):
    # capture passed in json_lists while replicating behaviour of iter_write_jsonl_to_file
    # this is a bit more difficult because the passed in iterable will be consumed
    json_lists.append([])
    for record in iter_write_jsonl_to_file(*args, **kwargs):
        json_lists[-1].append(record)
        yield record


@pytest.fixture(name='iter_write_jsonl_to_file_mock')
def _iter_write_jsonl_to_file_mock(
    iter_write_jsonl_to_file_mock_json_lists: List[List[dict]]
) -> Iterator[MagicMock]:
    with patch.object(
        generic_web_api_data_etl_module, 'iter_write_jsonl_to_file'
    ) as mock:
        mock.side_effect = functools.partial(
            _iter_write_jsonl_to_file_side_effect,
            json_lists=iter_write_jsonl_to_file_mock_json_lists
        )
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


@pytest.fixture(name='get_data_single_page_response_mock', autouse=True)
def _get_data_single_page_response_mock():
    with patch.object(
        generic_web_api_data_etl_module, 'get_data_single_page_response'
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


@pytest.fixture(name='iter_dict_for_bigquery_include_exclude_source_config_mock')
def _iter_dict_for_bigquery_include_exclude_source_config_mock():
    with patch.object(
        generic_web_api_data_etl_module, 'iter_dict_for_bigquery_include_exclude_source_config'
    ) as mock:
        yield mock


@pytest.fixture(name='process_web_api_data_etl_batch_with_batch_source_value_mock')
def _process_web_api_data_etl_batch_with_batch_source_value_mock():
    with patch.object(
        generic_web_api_data_etl_module, 'process_web_api_data_etl_batch_with_batch_source_value'
    ) as mock:
        yield mock


def get_data_config_with_max_source_values_per_request(
    data_config: WebApiConfig,
    max_source_values_per_request: int
) -> WebApiConfig:
    return dataclasses.replace(
        data_config,
        dynamic_request_builder=dataclasses.replace(
            data_config.dynamic_request_builder,
            max_source_values_per_request=max_source_values_per_request
        )
    )


def _remove_imported_timestamp_from_record_list(record_list: Iterable[dict]) -> Sequence[dict]:
    return [
        {
            key: value
            for key, value in record.items()
            if key != TEST_IMPORTED_TIMESTAMP_FIELD_NAME
        }
        for record in record_list
    ]


class TestGetStartTimestampFromStateFileOrOptionalDefaultValue:
    def test_should_return_none_without_state_config(self):
        data_config = get_data_config(WEB_API_CONFIG)
        assert get_start_timestamp_from_state_file_or_optional_default_value(data_config) is None

    def test_should_return_parsed_timestamp_from_state_file(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        data_config = get_data_config({
            **WEB_API_CONFIG,
            'stateFile': STATE_FILE_1
        })
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = TIMESTAMP_STRING_1
        assert (
            get_start_timestamp_from_state_file_or_optional_default_value(data_config)
        ) == TIMESTAMP_1

    def test_should_return_default_value_if_state_file_doesnt_exist(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        data_config = get_data_config({
            **WEB_API_CONFIG,
            'stateFile': STATE_FILE_1,
            'dataUrl': {
                **WEB_API_CONFIG['dataUrl'],
                'configurableParameters': {
                    'defaultStartDate': TIMESTAMP_STRING_1,
                    'dateFormat': r'%Y-%m-%d'
                }
            }
        })
        download_s3_object_as_string_or_file_not_found_error_mock.side_effect = FileNotFoundError
        assert (
            get_start_timestamp_from_state_file_or_optional_default_value(data_config)
        ) == TIMESTAMP_1

    def test_should_replace_placeholders(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        data_config = get_data_config({
            **WEB_API_CONFIG,
            'stateFile': {
                'bucketName': 'bucket_1',
                'objectName': r'prefix-{placeholder1}'
            }
        })
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = TIMESTAMP_STRING_1
        get_start_timestamp_from_state_file_or_optional_default_value(
            data_config,
            placeholder_values={'placeholder1': 'value1'}
        )
        download_s3_object_as_string_or_file_not_found_error_mock.assert_called_with(
            bucket='bucket_1',
            object_key='prefix-value1'
        )


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
        data_config = get_data_config({
            **WEB_API_CONFIG,
            'stateFile': STATE_FILE_1
        })
        upload_latest_timestamp_as_pipeline_state(
            data_config, latest_timestamp
        )
        state_file_bucket = STATE_FILE_1['bucketName'].replace('{ENV}', DEP_ENV)
        state_file_name_key = STATE_FILE_1['objectName'].replace('{ENV}', DEP_ENV)
        mock_upload_s3_object.assert_called_with(
            bucket=state_file_bucket,
            object_key=state_file_name_key,
            data_object=latest_timestamp_string,
        )

    def test_should_replace_placeholders_in_object_name(
        self,
        mock_upload_s3_object: MagicMock
    ):
        data_config = get_data_config({
            **WEB_API_CONFIG,
            'stateFile': {
                'bucketName': 'bucket_1',
                'objectName': r'prefix-{placeholder1}'
            }
        })
        upload_latest_timestamp_as_pipeline_state(
            data_config,
            TIMESTAMP_1,
            placeholder_values={'placeholder1': 'value1'}
        )
        mock_upload_s3_object.assert_called_with(
            bucket='bucket_1',
            object_key='prefix-value1',
            data_object=ANY,
        )


class TestGetOptionalTotalCount:
    def test_should_return_none_if_no_total_path_configured(self):
        data_config = get_data_config(WEB_API_CONFIG)
        data = {'total': 123}
        assert get_optional_total_count(data, data_config) is None

    def test_should_return_total_from_response(self):
        conf_dict = {
            **WEB_API_CONFIG,
            'response': {
                'totalItemsCountKeyFromResponseRoot': ['total']
            }
        }
        data_config = get_data_config(conf_dict)
        data = {'total': 123}
        assert get_optional_total_count(data, data_config) == 123


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
            **WEB_API_CONFIG,
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
            **WEB_API_CONFIG,
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
            **WEB_API_CONFIG,
            'response': {
                'itemsKeyFromResponseRoot': path_keys
            }
        }
        data_config = get_data_config(conf_dict)
        data = {'error': 'error message'}

        with pytest.raises(ValueError):
            get_items_list(data, data_config)


def _get_web_api_config_with_cursor_path(cursor_path: Sequence[str]) -> WebApiConfig:
    conf_dict: WebApiConfigDict = cast(WebApiConfigDict, {
        **WEB_API_CONFIG,
        'response': {
            'nextPageCursorKeyFromResponseRoot': cursor_path
        },
        'dataUrl': {
            **WEB_API_CONFIG['dataUrl'],
            'configurableParameters': {
                'nextPageCursorParameterName': 'cursor'
            }
        }
    })
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

    def test_should_ignore_get_cursor_value_when_matching_previous_cursor_by_default(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data = {'cursor_key1': 'cursor1'}
        with pytest.raises(RuntimeError):
            get_next_cursor_from_data(
                data, data_config, previous_cursor='cursor1'
            )

    def test_should_continue_on_same_same_cursor_if_configured(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data_config = dataclasses.replace(
            data_config,
            response=dataclasses.replace(
                data_config.response,
                on_same_next_cursor='Continue'
            )
        )
        data = {'cursor_key1': 'cursor1'}
        actual_next_cursor = get_next_cursor_from_data(
            data, data_config, previous_cursor='cursor1'
        )
        assert actual_next_cursor == 'cursor1'

    def test_should_stop_on_same_same_cursor_if_configured(self):
        data_config = _get_web_api_config_with_cursor_path(cursor_path=['cursor_key1'])
        data_config = dataclasses.replace(
            data_config,
            response=dataclasses.replace(
                data_config.response,
                on_same_next_cursor='Stop'
            )
        )
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


class TestGetDataSinglePage:
    def test_should_pass_method_url_and_header_to_session_request(
        self,
        requests_session_mock: MagicMock,
        get_response_and_provenance_from_api_mock: MagicMock
    ):
        dynamic_request_builder = MagicMock(name='dynamic_request_builder')
        dynamic_request_builder.method = 'POST'
        data_config = dataclasses.replace(
            get_data_config(WEB_API_CONFIG),
            dynamic_request_builder=dynamic_request_builder
        )
        get_data_single_page_response(
            data_config=data_config,
            dynamic_request_parameters=WebApiDynamicRequestParameters()
        )
        get_response_and_provenance_from_api_mock.assert_called_with(
            session=requests_session_mock,
            method=data_config.dynamic_request_builder.method,
            url=dynamic_request_builder.get_url.return_value,
            json_data=dynamic_request_builder.get_json.return_value,
            headers=data_config.headers.mapping,
            raise_on_status=True
        )

    def test_should_pass_dynamic_request_parameters_to_get_json(self):
        dynamic_request_builder = MagicMock(name='dynamic_request_builder')
        dynamic_request_builder.method = 'POST'
        data_config = dataclasses.replace(
            get_data_config(WEB_API_CONFIG),
            dynamic_request_builder=dynamic_request_builder
        )
        dynamic_request_parameters = WebApiDynamicRequestParameters(source_values=['value1'])
        get_data_single_page_response(
            data_config=data_config,
            dynamic_request_parameters=dynamic_request_parameters
        )
        dynamic_request_builder.get_json.assert_called_with(
            dynamic_request_parameters=dynamic_request_parameters
        )


class TestGetNextDynamicRequestParametersForPageData:
    def test_should_return_next_cursor(self):
        data_config = _get_web_api_config_with_cursor_path(['next-cursor'])
        initial_dynamic_request_parameters = WebApiDynamicRequestParameters(
            cursor=None
        )
        next_dynamic_request_parameters = get_next_dynamic_request_parameters_for_page_data(
            page_data={'next-cursor': 'cursor_2'},
            items_count=10,
            current_dynamic_request_parameters=initial_dynamic_request_parameters,
            data_config=data_config
        )
        assert next_dynamic_request_parameters
        assert next_dynamic_request_parameters.cursor == 'cursor_2'

    def test_should_keep_placeholder_values_when_using_cursor(self):
        data_config = _get_web_api_config_with_cursor_path(['next-cursor'])
        initial_dynamic_request_parameters = WebApiDynamicRequestParameters(
            cursor=None,
            placeholder_values=PLACEHOLDER_VALUES_1
        )
        next_dynamic_request_parameters = get_next_dynamic_request_parameters_for_page_data(
            page_data={'next-cursor': 'cursor_2'},
            items_count=10,
            current_dynamic_request_parameters=initial_dynamic_request_parameters,
            data_config=data_config
        )
        assert next_dynamic_request_parameters
        assert next_dynamic_request_parameters.placeholder_values == PLACEHOLDER_VALUES_1

    def test_should_return_next_source_values(self):
        data_config = get_data_config_with_max_source_values_per_request(
            get_data_config(WEB_API_CONFIG),
            max_source_values_per_request=2
        )
        all_source_values_iterator = iter(['value 1', 'value 2', 'value 3', 'value 4', 'value 5'])
        initial_dynamic_request_parameters = WebApiDynamicRequestParameters(
            cursor=None,
            source_values=list(itertools.islice(all_source_values_iterator, 2))
        )
        next_dynamic_request_parameters = get_next_dynamic_request_parameters_for_page_data(
            page_data={},
            items_count=1,
            current_dynamic_request_parameters=initial_dynamic_request_parameters,
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        assert next_dynamic_request_parameters
        assert list(next_dynamic_request_parameters.source_values) == ['value 3', 'value 4']

    def test_should_return_none_if_there_are_no_more_source_values(self):
        data_config = get_data_config_with_max_source_values_per_request(
            get_data_config(WEB_API_CONFIG),
            max_source_values_per_request=2
        )
        all_source_values_iterator = iter(['value 1', 'value 2'])
        initial_dynamic_request_parameters = WebApiDynamicRequestParameters(
            cursor=None,
            source_values=list(itertools.islice(all_source_values_iterator, 2))
        )
        next_dynamic_request_parameters = get_next_dynamic_request_parameters_for_page_data(
            page_data={},
            items_count=1,
            current_dynamic_request_parameters=initial_dynamic_request_parameters,
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        assert next_dynamic_request_parameters is None


class TestGetInitialDynamicRequestParameters:
    def test_should_set_source_values_to_none_if_not_provided(self):
        initial_dynamic_request_parameters = get_initial_dynamic_request_parameters(
            data_config=get_data_config(WEB_API_CONFIG),
            all_source_values_iterator=None
        )
        assert initial_dynamic_request_parameters.source_values is None

    def test_should_take_initial_batch_of_source_values(self):
        data_config = get_data_config_with_max_source_values_per_request(
            get_data_config(WEB_API_CONFIG),
            max_source_values_per_request=2
        )
        all_source_values_iterator = iter(['value 1', 'value 2', 'value 3'])
        initial_dynamic_request_parameters = get_initial_dynamic_request_parameters(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        assert list(initial_dynamic_request_parameters.source_values) == ['value 1', 'value 2']

    def test_should_return_none_if_source_values_are_empty(self):
        data_config = get_data_config_with_max_source_values_per_request(
            get_data_config(WEB_API_CONFIG),
            max_source_values_per_request=2
        )
        all_source_values_iterator = iter([])
        initial_dynamic_request_parameters = get_initial_dynamic_request_parameters(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        assert initial_dynamic_request_parameters is None

    def test_should_set_placeholder_values_to_none_by_default(self):
        initial_dynamic_request_parameters = get_initial_dynamic_request_parameters(
            data_config=get_data_config(WEB_API_CONFIG),
            placeholder_values=None
        )
        assert initial_dynamic_request_parameters.placeholder_values is None

    def test_should_pass_on_placeholder_values(self):
        initial_dynamic_request_parameters = get_initial_dynamic_request_parameters(
            data_config=get_data_config(WEB_API_CONFIG),
            placeholder_values=PLACEHOLDER_VALUES_1
        )
        assert initial_dynamic_request_parameters.placeholder_values == PLACEHOLDER_VALUES_1


class TestIterProcessedWebApiDataEtlBatchData:
    def test_should_paginate_and_return_all_processed_results_combined_and_stop_when_empty(
        self,
        get_data_single_page_response_mock: MagicMock
    ):
        conf_dict: WebApiConfigDict = cast(WebApiConfigDict, {
            **WEB_API_CONFIG,
            'dataUrl': {
                **WEB_API_CONFIG['dataUrl'],
                'configurableParameters': {
                    'nextPageCursorParameterName': 'cursor'
                }
            },
            'response': {
                'itemsKeyFromResponseRoot': ['rows'],
                'nextPageCursorKeyFromResponseRoot': ['cursor']
            }
        })
        data_config = get_data_config(conf_dict)
        LOGGER.debug('data_config: %r', data_config)
        item_lists_with_data = [
            [{'key_1': 'value 1'}],
            [{'key_1': 'value 2'}],
            [{'key_1': 'value 3'}]
        ]
        expected_combined_item_list_with_data = [
            item
            for item_list in item_lists_with_data
            for item in item_list
        ]
        item_lists_incl_empty_and_dummy_data = item_lists_with_data + [
            [],  # empty results
            [{'key_1': 'should not be called'}],
        ]
        responses = [
            WebApiPageResponse(
                {'rows': item_list, 'cursor': f'next-{index}'}
            )
            for index, item_list in enumerate(item_lists_incl_empty_and_dummy_data)
        ]
        get_data_single_page_response_mock.side_effect = responses
        record_list = list(_remove_imported_timestamp_from_record_list(
            iter_processed_web_api_data_etl_batch_data(data_config)
        ))
        LOGGER.debug('record_list: %r', record_list)
        assert record_list == expected_combined_item_list_with_data

    def test_should_pass_placeholder_values_to_request_parameters(
        self,
        get_data_single_page_response_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        list(iter_processed_web_api_data_etl_batch_data(
            data_config=data_config,
            placeholder_values=PLACEHOLDER_VALUES_1
        ))
        get_data_single_page_response_mock.assert_called_with(
            data_config=data_config,
            dynamic_request_parameters=get_initial_dynamic_request_parameters(
                data_config=data_config,
                placeholder_values=PLACEHOLDER_VALUES_1
            )
        )


class TestProcessWebApiDataEtlBatchWithBatchSourceValueAndDateRange:
    def test_should_pass_batch_source_values_as_placeholders_to_update_state(
        self,
        upload_latest_timestamp_as_pipeline_state_mock: MagicMock,
        get_items_list_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_WITH_TIMESTAMP_FIELD_CONFIG_DICT)
        get_items_list_mock.return_value = [{'timestamp': TIMESTAMP_STRING_1}]
        process_web_api_data_etl_batch_with_batch_source_value_and_date_range(
            data_config=data_config,
            batch_source_value=SOURCE_VALUE_1
        )
        upload_latest_timestamp_as_pipeline_state_mock.assert_called_with(
            data_config=ANY,
            latest_record_timestamp=ANY,
            placeholder_values=SOURCE_VALUE_1
        )

    def test_should_pass_batch_source_values_as_placeholders_in_request_parameters(
        self,
        get_data_single_page_response_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        process_web_api_data_etl_batch_with_batch_source_value_and_date_range(
            data_config=data_config,
            batch_source_value=SOURCE_VALUE_1
        )
        get_data_single_page_response_mock.assert_called_with(
            data_config=data_config,
            dynamic_request_parameters=get_initial_dynamic_request_parameters(
                data_config=data_config,
                placeholder_values=SOURCE_VALUE_1
            )
        )


class TestProcessWebApiDataEtlBatchWithBatchSourceValue:
    def test_should_pass_batch_source_values_as_placeholders_to_load_state(
        self,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_WITH_TIMESTAMP_FIELD_CONFIG_DICT)
        process_web_api_data_etl_batch_with_batch_source_value(
            data_config=data_config,
            batch_source_value=SOURCE_VALUE_1
        )
        get_start_timestamp_from_state_file_or_optional_default_value_mock.assert_called_with(
            data_config=ANY,
            placeholder_values=SOURCE_VALUE_1
        )


class TestProcessWebApiDataEtlBatch:
    def test_should_pass_on_all_source_values_iterator_if_should_not_iterate_over_source_values(
        self,
        process_web_api_data_etl_batch_with_batch_source_value_mock: MagicMock
    ):
        default_data_config = get_data_config(WEB_API_CONFIG)
        data_config = dataclasses.replace(
            default_data_config,
            dynamic_request_builder=dataclasses.replace(
                default_data_config.dynamic_request_builder,
                max_source_values_per_request=2
            )
        )
        all_source_values_iterator = iter([SOURCE_VALUE_1])
        process_web_api_data_etl_batch(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        process_web_api_data_etl_batch_with_batch_source_value_mock.assert_called_with(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator,
            batch_source_value=None
        )

    def test_should_iterate_over_source_values_and_pass_on_batch_source_value(
        self,
        process_web_api_data_etl_batch_with_batch_source_value_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        all_source_values_iterator = iter([SOURCE_VALUE_1])
        process_web_api_data_etl_batch(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        process_web_api_data_etl_batch_with_batch_source_value_mock.assert_called_with(
            data_config=data_config,
            all_source_values_iterator=None,
            batch_source_value=SOURCE_VALUE_1
        )


class TestGenericWebApiDataEtl:
    def test_should_pass_null_value_removed_item_list_to_iter_processed_record(
        self,
        get_items_list_mock: MagicMock,
        iter_processed_record_for_api_item_list_response_mock: MagicMock
    ):
        data_config = get_data_config(WEB_API_CONFIG)
        get_items_list_mock.return_value = [{'key_1': ['value1'], 'key_2': []}]
        generic_web_api_data_etl(data_config)
        iter_processed_record_for_api_item_list_response_mock.assert_called()
        _, kwargs = iter_processed_record_for_api_item_list_response_mock.call_args
        assert list(kwargs['record_list']) == [{'key_1': ['value1']}]

    def test_should_batch_data_written_to_bigquery(
        self,
        get_items_list_mock: MagicMock,
        iter_write_jsonl_to_file_mock: MagicMock,
        iter_write_jsonl_to_file_mock_json_lists: List[List[dict]]
    ):
        data_config = get_data_config(cast(WebApiConfigDict, {
            **WEB_API_CONFIG,
            'batchSize': 2
        }))
        get_items_list_mock.return_value = [
            {'key': 'record_1'}, {'key': 'record_2'}, {'key': 'record_3'}
        ]

        generic_web_api_data_etl(data_config)

        iter_write_jsonl_to_file_mock.assert_called()
        actual_key_lists = [
            [record['key'] for record in json_list]
            for json_list in iter_write_jsonl_to_file_mock_json_lists
        ]
        LOGGER.debug('actual_key_lists: %r', actual_key_lists)
        assert actual_key_lists == [
            ['record_1', 'record_2'],
            ['record_3']
        ]

    def test_should_update_state_file(
        self,
        get_items_list_mock: MagicMock,
        upload_latest_timestamp_as_pipeline_state_mock: MagicMock
    ):
        timestamp_string = '2020-01-01T01:01:01+00:00'
        timestamp = datetime.fromisoformat(timestamp_string)
        data_config = get_data_config(WEB_API_WITH_TIMESTAMP_FIELD_CONFIG_DICT)
        get_items_list_mock.return_value = [{'timestamp': timestamp_string}]
        generic_web_api_data_etl(data_config)
        upload_latest_timestamp_as_pipeline_state_mock.assert_called_with(
            data_config=data_config,
            latest_record_timestamp=timestamp,
            placeholder_values=None
        )

    def test_should_not_update_state_with_empty_list_in_response(
        self,
        get_data_single_page_response_mock: MagicMock,
        upload_latest_timestamp_as_pipeline_state_mock: MagicMock
    ):
        conf_dict: WebApiConfigDict = cast(WebApiConfigDict, {
            **WEB_API_CONFIG,
            'response': {
                'itemsKeyFromResponseRoot': ['rows']
            }
        })
        data_config = get_data_config(conf_dict)
        get_data_single_page_response_mock.return_value = WebApiPageResponse(
            {'rows': []}
        )
        generic_web_api_data_etl(data_config)
        upload_latest_timestamp_as_pipeline_state_mock.assert_not_called()

    def test_should_retrieve_data_in_date_range_batches(
        self,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock,
        get_data_single_page_response_mock: MagicMock,
        get_current_timestamp_mock: MagicMock
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
                response=WebApiResponseConfig(
                    item_timestamp_key_path_from_item_root=['timestamp']
                ),
                start_to_end_date_diff_in_days=batch_size_in_days,
                default_start_date=timestamp_string_1
            )
        )
        get_start_timestamp_from_state_file_or_optional_default_value_mock.return_value = (
            initial_timestamp
        )
        item_list = [{'timestamp': timestamp_string_2}]
        get_data_single_page_response_mock.return_value = WebApiPageResponse(item_list)
        get_current_timestamp_mock.return_value = end_timestamp
        generic_web_api_data_etl(data_config)
        actual_from_and_until_date_list = [
            (
                call_args.kwargs['dynamic_request_parameters'].from_date,
                call_args.kwargs['dynamic_request_parameters'].to_date
            )
            for call_args in get_data_single_page_response_mock.call_args_list
        ]
        assert actual_from_and_until_date_list == expected_from_and_until_date_list

    def test_should_pass_none_from_and_until_dates_if_not_configured(
        self,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock,
        get_data_single_page_response_mock: MagicMock
    ):
        expected_from_and_until_date_list = [(None, None)]
        data_config = get_data_config(WEB_API_CONFIG)
        get_start_timestamp_from_state_file_or_optional_default_value_mock.return_value = None
        item_list = [{'key': 'value'}]
        get_data_single_page_response_mock.return_value = WebApiPageResponse(item_list)
        generic_web_api_data_etl(data_config)
        actual_from_and_until_date_list = [
            (
                call_args.kwargs['dynamic_request_parameters'].from_date,
                call_args.kwargs['dynamic_request_parameters'].to_date
            )
            for call_args in get_data_single_page_response_mock.call_args_list
        ]
        assert actual_from_and_until_date_list == expected_from_and_until_date_list

    def test_should_load_source_values_from_bigquery_and_pass_to_get_data_single_page(
        self,
        iter_dict_for_bigquery_include_exclude_source_config_mock: MagicMock,
        get_start_timestamp_from_state_file_or_optional_default_value_mock: MagicMock,
        get_data_single_page_response_mock: MagicMock
    ):
        get_start_timestamp_from_state_file_or_optional_default_value_mock.return_value = None
        conf_dict: WebApiConfigDict = cast(WebApiConfigDict, {
            **WEB_API_CONFIG,
            'source': {'include': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1}}
        })
        data_config = get_data_config_with_max_source_values_per_request(
            get_data_config(conf_dict),
            max_source_values_per_request=2
        )
        iter_dict_for_bigquery_include_exclude_source_config_mock.return_value = iter(['value 1'])

        generic_web_api_data_etl(data_config)

        iter_dict_for_bigquery_include_exclude_source_config_mock.assert_called()
        get_data_single_page_response_mock.assert_called()
        _, kwargs = get_data_single_page_response_mock.call_args
        assert list(kwargs['dynamic_request_parameters'].source_values) == ['value 1']
