from datetime import timezone

from data_pipeline.s3_csv_data.s3_csv_state import (
    CsvState,
    ObjectPatternCsvState,
    parse_timestamp
)


TIMESTAMP_STRING_1 = '2020-01-01T00:00:00+00:00'
TIMESTAMP_STRING_2 = '2020-01-02T00:00:00+00:00'

TIMESTAMP_1 = parse_timestamp(TIMESTAMP_STRING_1)
TIMESTAMP_2 = parse_timestamp(TIMESTAMP_STRING_2)

OBJECT_PATTERN_1 = 'object_pattern_1*'


class TestParseTimestamp:
    def test_should_parse_old_datetime_format(self):
        timestamp = parse_timestamp(
            '2001-02-03 04:05:06'
        )
        assert timestamp.date().isoformat() == '2001-02-03'
        assert timestamp.tzinfo == timezone.utc

    def test_should_parse_timestamp(self):
        timestamp = parse_timestamp(
            '2001-02-03T05:06:07+00:00'
        )
        assert timestamp.date().isoformat() == '2001-02-03'
        assert timestamp.tzinfo == timezone.utc


class TestCsvState:
    def test_should_load_from_empty_dict(self):
        state = CsvState.from_dict({})
        assert not state.state_dict

    def test_should_parse_timestamp(self):
        state = CsvState.from_dict({
            OBJECT_PATTERN_1: TIMESTAMP_STRING_1
        })
        assert state.state_dict == {
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_timestamp=TIMESTAMP_1
            )
        }

    def test_should_serialize_timestamp_to_string(self):
        state = CsvState(state_dict={
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_timestamp=TIMESTAMP_1
            )
        })
        assert state.to_dict() == {
            OBJECT_PATTERN_1: TIMESTAMP_1.isoformat()
        }

    def test_should_update_timestamp_for_object_pattern(self):
        state = CsvState(state_dict={
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_timestamp=TIMESTAMP_1
            )
        })
        state.update_last_modified_timestamp(
            object_pattern=OBJECT_PATTERN_1,
            last_modified_timestamp=TIMESTAMP_2
        )
        assert state.state_dict[OBJECT_PATTERN_1].last_modified_timestamp == (
            TIMESTAMP_2
        )

    def test_should_return_initial_state(self):
        state = CsvState.get_initial_state(
            object_patterns=[OBJECT_PATTERN_1],
            last_modified_timestamp=TIMESTAMP_1
        )
        assert state.state_dict == {
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_timestamp=TIMESTAMP_1
            )
        }
