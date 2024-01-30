import html
import json
from typing import Any, Callable, Mapping


RecordProcessingStepFunction = Callable[[Any], Any]


def unescape_html_escaped_values_in_string(val):
    n_val = val
    if isinstance(val, str):
        n_val = html.unescape(val)
    return n_val


def strip_quotes(val):
    n_val = val
    if isinstance(val, str):
        for to_strip_away in ["'", '"']:
            n_val = n_val.strip()
            if n_val.endswith(to_strip_away) and n_val.startswith(
                    to_strip_away
            ):
                n_val = n_val.strip(to_strip_away)
    return n_val


def parse_json_value(value: str):
    if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
        return json.loads(value)
    return value


FUNCTION_NAME_MAPPING: Mapping[str, RecordProcessingStepFunction] = {
    'html_unescape': unescape_html_escaped_values_in_string,
    'strip_quotes': strip_quotes,
    'parse_json_value': parse_json_value
}
