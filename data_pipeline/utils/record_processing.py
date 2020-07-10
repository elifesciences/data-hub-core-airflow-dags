import html
from typing import List, Iterable

import dateparser

from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    standardize_field_name,
    convert_bq_schema_field_list_to_dict
)


DEFAULT_PROCESSING_STEPS = ['strip_quotes']
BQ_SCHEMA_FIELD_NAME_KEY = "name"
BQ_SCHEMA_SUBFIELD_KEY = "fields"
BQ_SCHEMA_FIELD_TYPE_KEY = "type"


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


def add_provenance_to_record(
        record: dict,
        current_etl_time: str,
        timestamp_field_name: str = 'data_import_timestamp',
        provenance_field_name: str = 'provenance',
        annotation_field_name: str = 'annotation',
        record_annotation: dict = None
):
        provenance = {
            provenance_field_name: {
                timestamp_field_name: current_etl_time,
                annotation_field_name: record_annotation
            }
        }
        return {
            **record,
            **provenance
        }


def iter_add_provenance(
        iter_records: Iterable[dict],
        current_etl_time: str,
        timestamp_field_name: str = 'data_import_timestamp',
        provenance_field_name: str = 'provenance',
        annotation_field_name: str = 'annotation',
        record_annotation: dict = None
):
    for record in iter_records:

        yield add_provenance_to_record(
            record, current_etl_time, timestamp_field_name,
            provenance_field_name, annotation_field_name,
            record_annotation
        )


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def standardize_record_keys(record_object):
    if isinstance(record_object, dict):
        new_dict = {}
        for item_key, item_val in record_object.items():
            new_key = standardize_field_name(item_key)
            if isinstance(item_val, (list, dict)):
                item_val = standardize_record_keys(
                    item_val,
                )
            if item_val:
                new_dict[new_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = list()
        for elem in record_object:
            if isinstance(elem, (dict, list)):
                elem = standardize_record_keys(
                    elem
                )
            if elem is not None:
                new_list.append(elem)
        return new_list


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def filter_record_by_schema(record_object, record_object_schema):
    if isinstance(record_object, dict):
        list_as_p_dict = convert_bq_schema_field_list_to_dict(
            record_object_schema
        )
        key_list = set(list_as_p_dict.keys())
        new_dict = {}
        for item_key, item_val in record_object.items():
            if item_key in key_list:
                if isinstance(item_val, (list, dict)):
                    item_val = filter_record_by_schema(
                        item_val,
                        list_as_p_dict.get(item_key).get(
                            BQ_SCHEMA_SUBFIELD_KEY
                        ),
                    )
                if (
                        list_as_p_dict.get(item_key)
                        .get(BQ_SCHEMA_FIELD_TYPE_KEY)
                        .lower() == "timestamp"
                ):
                    try:
                        dateparser.parse(
                            item_val
                        )
                    except BaseException:
                        item_val = None
                new_dict[item_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = list()
        for elem in record_object:
            if isinstance(elem, (dict, list)):
                elem = filter_record_by_schema(
                    elem, record_object_schema
                )
            if elem is not None:
                new_list.append(elem)
        return new_list


FUNCTION_NAME_MAPPING = {
    "html_unescape": unescape_html_escaped_values_in_string,
    "strip_quotes": strip_quotes
}
