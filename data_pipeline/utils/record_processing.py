from typing import Any, Callable, List

from data_pipeline.utils.record_processing_functions import FUNCTION_NAME_MAPPING


DEFAULT_PROCESSING_STEPS = ['strip_quotes']


RecordProcessingStepFunction = Callable[[Any], Any]


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
