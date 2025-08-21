from datetime import timezone

from data_pipeline.s3_csv_data.s3_csv_state import (
    CsvState,
    ObjectPatternCsvState,
    convert_datetime_string_to_datetime
)


DATETTIME_STRING_1 = '2020-01-01 00:00:00'
DATETTIME_STRING_2 = '2020-01-02 00:00:00'

DATETTIME_1 = convert_datetime_string_to_datetime(DATETTIME_STRING_1)
DATETTIME_2 = convert_datetime_string_to_datetime(DATETTIME_STRING_2)

OBJECT_PATTERN_1 = 'object_pattern_1*'


class TestConvertDatetimeStringToDatetime:
    def test_should_parse_old_datetime_format(self):
        timestamp = convert_datetime_string_to_datetime(
            '2001-02-03 04:05:06'
        )
        assert timestamp.date().isoformat() == '2001-02-03'
        assert timestamp.tzinfo == timezone.utc

    def test_should_parse_timestamp(self):
        timestamp = convert_datetime_string_to_datetime(
            '2001-02-03T05:06:07+00:00'
        )
        assert timestamp.date().isoformat() == '2001-02-03'
        assert timestamp.tzinfo == timezone.utc


class TestCsvState:
    def test_should_load_from_empty_dict(self):
        state = CsvState.from_dict({})
        assert not state.state_dict

    def test_should_parse_datetime(self):
        state = CsvState.from_dict({
            OBJECT_PATTERN_1: DATETTIME_STRING_1
        })
        assert state.state_dict == {
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_datetime=DATETTIME_1
            )
        }

    def test_should_serialize_datetime_to_string(self):
        state = CsvState(state_dict={
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_datetime=DATETTIME_1
            )
        })
        assert state.to_dict() == {
            OBJECT_PATTERN_1: DATETTIME_1.isoformat()
        }

    def test_should_update_datetime_for_object_pattern(self):
        state = CsvState(state_dict={
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_datetime=DATETTIME_1
            )
        })
        state.update_last_modified_datetime(
            object_pattern=OBJECT_PATTERN_1,
            last_modified_datetime=DATETTIME_2
        )
        assert state.state_dict[OBJECT_PATTERN_1].last_modified_datetime == (
            DATETTIME_2
        )

    def test_should_return_initial_state(self):
        state = CsvState.get_initial_state(
            object_patterns=[OBJECT_PATTERN_1],
            last_modified_datetime=DATETTIME_1
        )
        assert state.state_dict == {
            OBJECT_PATTERN_1: ObjectPatternCsvState(
                last_modified_datetime=DATETTIME_1
            )
        }
