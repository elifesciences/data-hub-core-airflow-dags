from datetime import datetime, timezone

import pytest

from data_pipeline.utils.data_pipeline_timestamp import parse_timestamp


class TestParseTimestamp:
    def test_should_parse_timestamp_with_timeoffset(self):
        timestamp_str = '2023-10-01T12:30:45+00:00'
        expected_datetime = datetime(2023, 10, 1, 12, 30, 45, tzinfo=timezone.utc)
        parsed_datetime = parse_timestamp(timestamp_str)
        assert parsed_datetime == expected_datetime

    def test_should_parse_timestamp_with_z(self):
        timestamp_str = '2023-10-01T12:30:45Z'
        expected_datetime = datetime(2023, 10, 1, 12, 30, 45, tzinfo=timezone.utc)
        parsed_datetime = parse_timestamp(timestamp_str)
        assert parsed_datetime == expected_datetime

    def test_should_reject_timestamp_without_timezone(self):
        timestamp_str = '2023-10-01T12:30:45'
        with pytest.raises(ValueError):
            parse_timestamp(timestamp_str)

    def test_should_reject_invalid_timestamp(self):
        timestamp_str = '2023-10-01xx12:30:45Z'
        with pytest.raises(ValueError):
            parse_timestamp(timestamp_str)
