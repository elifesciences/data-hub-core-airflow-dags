from data_pipeline.s3_csv_data.s3_csv_etl import convert_datetime_string_to_datetime
from data_pipeline.s3_csv_data.s3_csv_state import CsvState, ObjectPatternCsvState


DATETTIME_STRING_1 = '2020-01-01 00:00:00'

DATETTIME_1 = convert_datetime_string_to_datetime(DATETTIME_STRING_1)

OBJECT_PATTERN_1 = 'object_pattern_1*'


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
            OBJECT_PATTERN_1: DATETTIME_STRING_1
        }
