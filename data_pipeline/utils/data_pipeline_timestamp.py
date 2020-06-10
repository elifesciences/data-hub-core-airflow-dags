from datetime import (
    datetime,
    timezone
)


def get_current_timestamp_as_string(
        time_format: str = "%Y-%m-%dT%H:%M:%SZ"
):
    dtobj = datetime.now(timezone.utc)
    return dtobj.strftime(time_format)


def datetime_to_string(
        datetime_obj: datetime = None,
        datetime_format: str = None
):
    return datetime_obj.strftime(datetime_format) if datetime_obj else None
