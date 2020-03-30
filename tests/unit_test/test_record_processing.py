from data_pipeline.utils.record_processing import (
    unescape_html_escaped_values_in_string,
    strip_quotes,
    process_record_values
)


def test_should_unescape_html_should_return_unescaped_values():
    test_data = ["&quot;", "&amp;", "&lt", "&gt"]
    expected_result = ["\"", "&", "<", ">"]
    result = [
        unescape_html_escaped_values_in_string(val)
        for val in test_data
    ]
    assert result == expected_result


def test_should_strip_when_qoute_is_at_both_ends():
    test_data = [" 'test1' ", "\"test2\""]
    expected_result = ["test1", "test2"]
    result = [
        strip_quotes(val)
        for val in test_data
    ]
    assert result == expected_result


def test_should_not_strip_when_quote_is_at_only_one_end():
    test_data = [" 'test1 ", "test2\""]
    expected_result = ["'test1", "test2\""]
    result = [
        strip_quotes(val)
        for val in test_data
    ]
    assert result == expected_result


def test_should_unescape_html_for_nested_record():
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
