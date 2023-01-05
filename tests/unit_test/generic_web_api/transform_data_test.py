from data_pipeline.generic_web_api.transform_data import (
    filter_record_by_schema
)


class TestFilterRecordBySchema:
    def test_should_keep_timestamp_if_valid(self):
        result = filter_record_by_schema(
            record_object={'ts': '2023-01-01T00:00:00Z'},
            record_object_schema=[
                {
                    "mode": "NULLABLE",
                    "name": "ts",
                    "type": "TIMESTAMP"
                }
            ]
        )
        assert result == {'ts': '2023-01-01T00:00:00Z'}

    def test_should_set_timestamp_to_none_if_invalid_format(self):
        result = filter_record_by_schema(
            record_object={'ts': '-123'},
            record_object_schema=[
                {
                    "mode": "NULLABLE",
                    "name": "ts",
                    "type": "TIMESTAMP"
                }
            ]
        )
        assert result == {'ts': None}

    def test_should_set_timestamp_to_none_if_invalid_value(self):
        result = filter_record_by_schema(
            record_object={'ts': '0000-01-01T00:00:00Z'},
            record_object_schema=[
                {
                    "mode": "NULLABLE",
                    "name": "ts",
                    "type": "TIMESTAMP"
                }
            ]
        )
        assert result == {'ts': None}

    def test_should_set_timestamp_to_none_if_invalid_type(self):
        result = filter_record_by_schema(
            record_object={'ts': -123},
            record_object_schema=[
                {
                    "mode": "NULLABLE",
                    "name": "ts",
                    "type": "TIMESTAMP"
                }
            ]
        )
        assert result == {'ts': None}
