from data_pipeline.generic_web_api.transform_data import (
    filter_record_by_schema
)


TIMESTAMP_FIELD_NAME = 'ts'
TIMESTAMP_FIELD_SCHEMA = {
    'mode': 'NULLABLE',
    'name': TIMESTAMP_FIELD_NAME,
    'type': 'TIMESTAMP'
}


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
