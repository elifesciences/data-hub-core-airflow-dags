import html
import json
from typing import List

DEFAULT_PROCESSING_STEPS = ['strip_quotes']


def process_record_values(record, function_list: List[str]):
    if isinstance(record, dict) and function_list:
        for key, val in record.items():
            if isinstance(val, (dict, list)):
                process_record_values(val, function_list)
            else:
                record[key] = process_val_in_sequence(val, function_list)

    elif isinstance(record, list) and function_list:
        for index, val in enumerate(record):
            if isinstance(val, (dict, list)):
                process_record_values(val, function_list)
            else:
                record[index] = process_val_in_sequence(val, function_list)
    return record


def process_val_in_sequence(val, function_list):
    n_val = val
    if function_list:
        for func in function_list:
            if FUNCTION_NAME_MAPPING.get(func):
                n_val = FUNCTION_NAME_MAPPING.get(func)(n_val)
    return n_val


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


FUNCTION_NAME_MAPPING = {
    "html_unescape": unescape_html_escaped_values_in_string,
    "strip_quotes": strip_quotes,
    "parse_json_value": parse_json_value
}
