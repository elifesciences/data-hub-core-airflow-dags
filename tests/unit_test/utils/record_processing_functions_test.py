from data_pipeline.utils.record_processing_functions import (
    ChainedRecordProcessingStepFunction,
    get_resolved_record_processing_step_functions,
    get_single_record_processing_step_function_for_function_names_or_none,
    parse_json_value,
    transform_crossref_api_date_parts,
    unescape_html_escaped_values_in_string,
    strip_quotes
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


class TestTransformCrossrefApiDateParts:
    def test_should_return_passed_in_string_value(self):
        assert transform_crossref_api_date_parts('test') == 'test'

    def test_should_return_passed_in_dict_without_date_parts(self):
        assert transform_crossref_api_date_parts(
            {'key_1': 'value 1'}
        ) == {'key_1': 'value 1'}

    def test_should_replace_date_parts_with_dict(self):
        assert transform_crossref_api_date_parts(
            {'date-parts': [[2001, 2, 3]]}
        ) == {'date-parts': {'year': 2001, 'month': 2, 'day': 3}}

    def test_should_retain_other_keys(self):
        assert transform_crossref_api_date_parts(
            {'date-parts': [[2001, 2, 3]], 'other_key': 'other_value'}
        ) == {'date-parts': {'year': 2001, 'month': 2, 'day': 3}, 'other_key': 'other_value'}


class TestChainedRecordProcessingStepFunction:
    def test_should_return_passed_in_value_with_empty_processing_functions(self):
        record_processing_function = ChainedRecordProcessingStepFunction([])
        assert record_processing_function('test') == 'test'

    def test_should_use_passed_in_functions(self):
        record_processing_function = ChainedRecordProcessingStepFunction([
            lambda value: f'f1:{value}',
            lambda value: f'f2:{value}'
        ])
        assert record_processing_function('test') == 'f2:f1:test'


class TestGetResolvedRecordProcessingStepFunctions:
    def test_should_return_empty_list_for_none(self):
        assert get_resolved_record_processing_step_functions(None) == []

    def test_should_return_resolved_function(self):
        assert get_resolved_record_processing_step_functions([
            'html_unescape'
        ]) == [unescape_html_escaped_values_in_string]


class TestGetSingleRecordProcessingStepFunctionForFunctionNames:
    def test_should_return_chain_function_with_resolved_functions(self):
        record_processing_step_function = (
            get_single_record_processing_step_function_for_function_names_or_none([
                'html_unescape'
            ])
        )
        assert isinstance(record_processing_step_function,  ChainedRecordProcessingStepFunction)
        assert record_processing_step_function.record_processing_functions == [
            unescape_html_escaped_values_in_string
        ]
