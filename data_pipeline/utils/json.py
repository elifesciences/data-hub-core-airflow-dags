from datetime import datetime
from typing import Any, Callable, Optional, Tuple, TypeVar
import pandas as pd
import numpy as np

T = TypeVar('T')


def get_json_compatible_value(value):
    """
    Returns a value that can be JSON serialized.
    This is more or less identical to JSONEncoder.default,
    but less dependent on the actual serialization.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def get_recursive_json_compatible_value(value):
    if isinstance(value, dict):
        return {
            key: get_recursive_json_compatible_value(item_value)
            for key, item_value in value.items()
        }
    if isinstance(value, list):
        return [
            get_recursive_json_compatible_value(item_value)
            for item_value in value
        ]
    return get_json_compatible_value(value)


def get_recursively_transformed_object(
    obj: Any,
    key_value_transform_fn: Callable[
        [str, Any],
        Tuple[Optional[str], Optional[Any]]
    ]
) -> Any:
    if isinstance(obj, dict):
        return {
            updated_key: updated_value
            for key, value in obj.items()
            for updated_key, updated_value in [
                key_value_transform_fn(
                    key,
                    get_recursively_transformed_object(
                        value,
                        key_value_transform_fn=key_value_transform_fn
                    )
                )
            ]
            if updated_key
        }
    if isinstance(obj, list):
        return [
            get_recursively_transformed_object(
                value,
                key_value_transform_fn=key_value_transform_fn
            )
            for value in obj
        ]
    return obj


def is_empty_value(value) -> bool:
    try:
        if not len(value):  # pylint: disable=use-implicit-booleaness-not-len
            return True
    except TypeError:
        pass
    return (
        (value is None or np.isscalar(value))
        and (
            pd.isnull(value)
            or (not value and not isinstance(value, bool))
        )
    )


def get_recursively_filtered_dict_items_where_value(
    record: T,
    condition: Callable[[Any], bool]
) -> T:
    if isinstance(record, dict):
        first_pass_result = {
            key: get_recursively_filtered_dict_items_where_value(
                value,
                condition=condition
            )
            for key, value in record.items()
            if condition(value)
        }
        # second pass to remove anything that may not pass the condition after calling
        # the filter function
        return {  # type: ignore
            key: value
            for key, value in first_pass_result.items()
            if condition(value)
        }
    if isinstance(record, list):
        first_pass_result = [  # type: ignore
            get_recursively_filtered_dict_items_where_value(
                value,
                condition=condition
            )
            for value in record
            if condition(value)
        ]
        return [  # type: ignore
            value
            for value in first_pass_result
            if condition(value)
        ]
    return record


def remove_key_with_null_value(record: T) -> T:
    return get_recursively_filtered_dict_items_where_value(
        record,
        lambda value: not is_empty_value(value)
    )
