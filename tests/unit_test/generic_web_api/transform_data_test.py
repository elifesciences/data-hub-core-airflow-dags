import dataclasses
import pytest
from data_pipeline.generic_web_api.generic_web_api_config import WebApiResponseConfig

from data_pipeline.generic_web_api.transform_data import (
    PROVENANCE_FIELD_NAME,
    filter_record_by_schema,
    get_dict_values_from_path_as_list,
    get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root,
    get_web_api_provenance,
    iter_processed_record_for_api_item_list_response,
    process_record_in_list
)
from data_pipeline.utils.data_pipeline_timestamp import parse_timestamp_from_str

from tests.unit_test.generic_web_api.test_data import (
    MINIMAL_WEB_API_CONFIG_DICT,
    get_data_config
)


TIMESTAMP_FIELD_NAME = 'ts'
TIMESTAMP_FIELD_SCHEMA = {
    'mode': 'NULLABLE',
    'name': TIMESTAMP_FIELD_NAME,
    'type': 'TIMESTAMP'
}

TIMESTAMP_STR_1 = '2001-01-01T00:00:00+00:00'
TIMESTAMP_STR_2 = '2001-01-02T00:00:00+00:00'
TIMESTAMP_STR_3 = '2001-01-03T00:00:00+00:00'
TIMESTAMP_STR_4 = '2001-01-04T00:00:00+00:00'


REQUEST_PROVENANCE_1 = {
    'api_url': 'url-1'
}


class TestGetDictValuesFromPathAsList:
    def test_should_return_value_from_dict_by_key(self):
        assert get_dict_values_from_path_as_list(
            {'key': 'value'},
            ['key']
        ) == 'value'

    def test_should_return_value_from_nested_dict_by_key(self):
        assert get_dict_values_from_path_as_list(
            {'parent': {'key': 'value'}},
            ['parent', 'key']
        ) == 'value'

    def test_should_return_values_from_list_by_key(self):
        assert get_dict_values_from_path_as_list(
            [{'key': 'value'}],
            ['key']
        ) == ['value']


class TestGetLatestRecordListTimestampForItemTimestampKeyPathFromItemRoot:
    def test_should_parse_and_return_latest_timestamp(self):
        result = get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
            [
                {'ts': TIMESTAMP_STR_2},
                {'ts': TIMESTAMP_STR_4},
                {'ts': TIMESTAMP_STR_3}
            ],
            previous_latest_timestamp=None,
            item_timestamp_key_path_from_item_root=['ts']
        )
        assert result == parse_timestamp_from_str(TIMESTAMP_STR_4)

    def test_should_return_previous_timestamp_if_latest(self):
        result = get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
            [
                {'ts': TIMESTAMP_STR_1},
                {'ts': TIMESTAMP_STR_3},
                {'ts': TIMESTAMP_STR_2}
            ],
            previous_latest_timestamp=parse_timestamp_from_str(TIMESTAMP_STR_4),
            item_timestamp_key_path_from_item_root=['ts']
        )
        assert result == parse_timestamp_from_str(TIMESTAMP_STR_4)

    def test_should_return_latest_timestamp_from_nested_list(self):
        result = get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
            [{
                'data': [
                    {'ts': TIMESTAMP_STR_2},
                    {'ts': TIMESTAMP_STR_4},
                    {'ts': TIMESTAMP_STR_3}
                ]
            }],
            previous_latest_timestamp=None,
            item_timestamp_key_path_from_item_root=['data', 'ts']
        )
        assert result == parse_timestamp_from_str(TIMESTAMP_STR_4)

    def test_should_ignore_empty_timestamp_str(self):
        result = get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
            [{
                'data': [
                    {'ts': ''},
                    {'ts': TIMESTAMP_STR_4},
                    {'ts': TIMESTAMP_STR_3}
                ]
            }],
            previous_latest_timestamp=None,
            item_timestamp_key_path_from_item_root=['data', 'ts']
        )
        assert result == parse_timestamp_from_str(TIMESTAMP_STR_4)

    def test_should_consume_iterable_even_without_item_timestamp_key_path_from_item_root(self):
        # currently, the function is expected to consume the iterable in any case
        # this will ensure the records are written to a file
        record_list = [{'key': 'value'}]
        record_iterable = iter(record_list)
        get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
            record_iterable,
            previous_latest_timestamp=None,
            item_timestamp_key_path_from_item_root=None
        )
        remaining_items = list(record_iterable)
        assert not remaining_items

    def test_should_raise_error_if_key_was_not_found(self):
        with pytest.raises(KeyError):
            get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
                [{'ts': TIMESTAMP_STR_1}],
                previous_latest_timestamp=None,
                item_timestamp_key_path_from_item_root=['not_found']
            )


class TestFilterRecordBySchema:
    def test_should_keep_timestamp_if_valid(self):
        result = filter_record_by_schema(
            record_object={TIMESTAMP_FIELD_NAME: '2023-01-01T00:00:00Z'},
            record_object_schema=[TIMESTAMP_FIELD_SCHEMA]
        )
        assert result == {TIMESTAMP_FIELD_NAME: '2023-01-01T00:00:00Z'}

    def test_should_set_timestamp_to_none_if_invalid_format(self):
        result = filter_record_by_schema(
            record_object={TIMESTAMP_FIELD_NAME: 'z'},
            record_object_schema=[TIMESTAMP_FIELD_SCHEMA]
        )
        assert result == {TIMESTAMP_FIELD_NAME: None}

    def test_should_set_timestamp_to_none_if_invalid_value(self):
        result = filter_record_by_schema(
            record_object={TIMESTAMP_FIELD_NAME: '0000-01-01T00:00:00Z'},
            record_object_schema=[TIMESTAMP_FIELD_SCHEMA]
        )
        assert result == {TIMESTAMP_FIELD_NAME: None}

    def test_should_set_timestamp_to_none_if_invalid_type(self):
        result = filter_record_by_schema(
            record_object={TIMESTAMP_FIELD_NAME: 123},
            record_object_schema=[TIMESTAMP_FIELD_SCHEMA]
        )
        assert result == {TIMESTAMP_FIELD_NAME: None}


class TestProcessRecordInList:
    def test_should_select_fields_to_return(self):
        updated_records = list(process_record_in_list([{
            'key_1': 'value 1',
            'key_2': 'value 2'
        }], fields_to_return=['key_1']))
        assert updated_records == [{
            'key_1': 'value 1'
        }]

    def test_should_standardize_field_names(self):
        updated_records = list(process_record_in_list([{
            'key-1': 'value 1'
        }]))
        assert updated_records == [{
            'key_1': 'value 1'
        }]

    def test_should_add_provenance(self):
        updated_records = list(process_record_in_list([{
            'key_1': 'value 1'
        }], provenance={'provenance_field_1': 'provenance value 1'}))
        assert updated_records == [{
            'key_1': 'value 1',
            'provenance_field_1': 'provenance value 1'
        }]


class TestIterProcessedRecordForApiItemListResponse:
    def test_should_select_fields_to_return(self):
        updated_records = list(iter_processed_record_for_api_item_list_response(
            [{
                'key_1': 'value 1',
                'key_2': 'value 2'
            }],
            data_config=get_data_config({
                **MINIMAL_WEB_API_CONFIG_DICT,
                'response': {
                    'fieldsToReturn': ['key_1']
                }
            }),
            provenance={}
        ))
        assert updated_records == [{
            'key_1': 'value 1'
        }]

    def test_should_standardize_field_names(self):
        updated_records = list(iter_processed_record_for_api_item_list_response(
            [{
                'key-1': 'value 1'
            }],
            data_config=get_data_config(MINIMAL_WEB_API_CONFIG_DICT),
            provenance={}
        ))
        assert updated_records == [{
            'key_1': 'value 1'
        }]

    def test_should_add_provenance(self):
        updated_records = list(iter_processed_record_for_api_item_list_response(
            [{
                'key_1': 'value 1'
            }],
            data_config=get_data_config(MINIMAL_WEB_API_CONFIG_DICT),
            provenance={'provenance_field_1': 'provenance value 1'}
        ))
        assert updated_records == [{
            'key_1': 'value 1',
            'provenance_field_1': 'provenance value 1'
        }]

    def test_should_transform_values(self):
        default_config = get_data_config(MINIMAL_WEB_API_CONFIG_DICT)
        updated_records = list(iter_processed_record_for_api_item_list_response(
            [{'key_1': 'old value 1'}],
            data_config=dataclasses.replace(
                get_data_config(MINIMAL_WEB_API_CONFIG_DICT),
                response=dataclasses.replace(
                    default_config.response,
                    record_processing_step_function=(
                        lambda value: 'new value 1' if value == 'old value 1' else value
                    )
                )
            ),
            provenance={}
        ))
        assert updated_records == [{
            'key_1': 'new value 1'
        }]


class TestGetWebApiProvenance:
    def test_should_include_imported_timestamp_with_configured_key(self):
        data_config = get_data_config(MINIMAL_WEB_API_CONFIG_DICT)
        provenance_dict = get_web_api_provenance(
            data_config=data_config,
            data_etl_timestamp=TIMESTAMP_STR_1
        )
        assert provenance_dict == {
            data_config.import_timestamp_field_name: TIMESTAMP_STR_1
        }

    def test_should_not_include_provenance_if_disabled(self):
        data_config = dataclasses.replace(
            get_data_config(MINIMAL_WEB_API_CONFIG_DICT),
            response=WebApiResponseConfig(
                provenance_enabled=False
            )
        )
        provenance_dict = get_web_api_provenance(
            data_config=data_config,
            data_etl_timestamp=TIMESTAMP_STR_1,
            request_provenance=REQUEST_PROVENANCE_1
        )
        assert PROVENANCE_FIELD_NAME not in provenance_dict

    def test_should_include_request_provenance_if_enabled(self):
        data_config = dataclasses.replace(
            get_data_config(MINIMAL_WEB_API_CONFIG_DICT),
            response=WebApiResponseConfig(
                provenance_enabled=True
            )
        )
        provenance_dict = get_web_api_provenance(
            data_config=data_config,
            data_etl_timestamp=TIMESTAMP_STR_1,
            request_provenance=REQUEST_PROVENANCE_1
        )
        assert provenance_dict.get(PROVENANCE_FIELD_NAME) == REQUEST_PROVENANCE_1
