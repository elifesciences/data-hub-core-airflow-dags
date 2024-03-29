import html
import json
import logging
from typing import Any, Mapping, Optional, Protocol, Sequence


LOGGER = logging.getLogger(__name__)


class RecordProcessingStepFunction(Protocol):
    def __call__(self, value: Any) -> Any:
        pass


def unescape_html_escaped_values_in_string(value: Any) -> Any:
    n_val = value
    if isinstance(value, str):
        n_val = html.unescape(value)
    return n_val


def strip_quotes(value: Any) -> Any:
    n_val = value
    if isinstance(value, str):
        for to_strip_away in ["'", '"']:
            n_val = n_val.strip()
            if n_val.endswith(to_strip_away) and n_val.startswith(
                    to_strip_away
            ):
                n_val = n_val.strip(to_strip_away)
    return n_val


def parse_json_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
        return json.loads(value)
    return value


def transform_crossref_api_date_parts(value: Any) -> Any:
    LOGGER.debug('value: %r', value)
    if isinstance(value, dict):
        date_parts = value.get('date-parts')
        LOGGER.debug('date_parts: %r', date_parts)
        if date_parts:
            return {
                **value,
                'date-parts': dict(zip(['year', 'month', 'day'], date_parts[0]))
            }
    return value


class ChainedRecordProcessingStepFunction(RecordProcessingStepFunction):
    def __init__(self, record_processing_functions: Sequence[RecordProcessingStepFunction]):
        self.record_processing_functions = record_processing_functions

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.record_processing_functions})'

    def __call__(self, value: Any) -> Any:
        result = value
        for record_processing_function in self.record_processing_functions:
            result = record_processing_function(result)
        return result


FUNCTION_NAME_MAPPING: Mapping[str, RecordProcessingStepFunction] = {
    'html_unescape': unescape_html_escaped_values_in_string,
    'strip_quotes': strip_quotes,
    'parse_json_value': parse_json_value,
    'transform_crossref_api_date_parts': transform_crossref_api_date_parts
}


def get_resolved_record_processing_step_functions(
    record_processing_step_function_names: Optional[Sequence[str]]
) -> Sequence[RecordProcessingStepFunction]:
    LOGGER.debug(
        'record_processing_step_function_names: %r',
        record_processing_step_function_names
    )
    if not record_processing_step_function_names:
        return []
    return [
        FUNCTION_NAME_MAPPING[function_name]
        for function_name in record_processing_step_function_names
    ]


def get_single_record_processing_step_function_for_function_names_or_none(
    record_processing_step_function_names: Optional[Sequence[str]]
) -> Optional[RecordProcessingStepFunction]:
    if not record_processing_step_function_names:
        return None
    return ChainedRecordProcessingStepFunction(
        get_resolved_record_processing_step_functions(
            record_processing_step_function_names
        )
    )
