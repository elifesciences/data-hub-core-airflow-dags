import pytest

from data_pipeline.generic_web_api.transform_data import (
    filter_record_by_schema,
    get_dict_values_from_path_as_list,
    get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root
)
from data_pipeline.utils.data_pipeline_timestamp import parse_timestamp_from_str


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
