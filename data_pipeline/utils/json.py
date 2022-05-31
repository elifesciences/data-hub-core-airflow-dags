from datetime import datetime


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
