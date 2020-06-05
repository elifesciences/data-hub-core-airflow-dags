from datetime import (
    datetime,
    timezone
)


def get_current_timestamp_as_string(
        time_format: str = "%Y-%m-%dT%H:%M:%SZ"
):
    dtobj = datetime.now(timezone.utc)
    return dtobj.strftime(time_format)
