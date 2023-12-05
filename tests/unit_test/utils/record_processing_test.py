from data_pipeline.utils.record_processing import (
    parse_json_value,
    unescape_html_escaped_values_in_string,
    strip_quotes,
    process_record_values
)


class TestUnescapeHtmlEscapedValuesInString:
    def test_should_unescape_html_should_return_unescaped_values(self):
        test_data = ["&quot;", "&amp;", "&lt", "&gt"]
        expected_result = ["\"", "&", "<", ">"]
        result = [
            unescape_html_escaped_values_in_string(val)
            for val in test_data
        ]
        assert result == expected_result


class TestStripQuotes:
    def test_should_strip_when_quote_is_at_both_ends(self):
        test_data = [" 'test1' ", "\"test2\""]
        expected_result = ["test1", "test2"]
        result = [
            strip_quotes(val)
            for val in test_data
        ]
        assert result == expected_result

    def test_should_not_strip_when_quote_is_at_only_one_end(self):
        test_data = [" 'test1 ", "test2\""]
        expected_result = ["'test1", "test2\""]
        result = [
            strip_quotes(val)
            for val in test_data
        ]
        assert result == expected_result


class TestParseJsonValue:
    def test_should_return_none_if_passed_in_value_is_none(self):
        assert parse_json_value(None) is None

    def test_should_return_passed_in_value_if_not_string(self):
        assert parse_json_value(123) == 123

    def test_should_return_passed_in_value_if_not_starting_and_ending_with_curly_bracket(self):
        assert parse_json_value('test') == 'test'
        assert parse_json_value('{test') == '{test'
        assert parse_json_value('test}') == 'test}'

    def test_should_return_parse_json_value_if_starting_and_ending_with_curly_bracket(self):
        assert parse_json_value('{"key": "value"}') == {'key': 'value'}


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
