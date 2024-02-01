from data_pipeline.utils.record_processing import (
    process_record_values
)


class TestProcessRecordValues:
    def test_should_unescape_html_for_nested_record(self):
        data = {
            "a": "&pound;682m",
            "b": {
                "c": "d",
                "e": "&pound;682m",
                "f": ["a", "y", "&pound;682m", "z"],
                "g": [{"f": "k", "m": "&pound;682m"}, {"y": "l"}]
            }
        }
        expected_result = {
            'a': '£682m',
            'b': {
                'c': 'd', 'e': '£682m',
                'f': ['a', 'y', '£682m', 'z'],
                'g': [{'f': 'k', 'm': '£682m'}, {'y': 'l'}]
            }
        }
        actual_result = process_record_values(data, ["html_unescape"])
        assert actual_result == expected_result
