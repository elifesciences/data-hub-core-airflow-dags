import logging

from datetime import (
    datetime,
    timezone
)
import dateparser
import pytz

LOGGER = logging.getLogger(__name__)


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


def parse_timestamp_from_str(timestamp_as_str, time_format: str = None):
    if time_format:
        timestamp_obj = datetime.strptime(
            timestamp_as_str.strip(), time_format
        )
    else:
        timestamp_obj = dateparser.parse(
            timestamp_as_str
        )
    return timestamp_obj


def is_datetime_tz_aware(datetime_obj: datetime):
    return (
        datetime_obj.tzinfo is not None
        and datetime_obj.tzinfo.utcoffset(
            datetime_obj
        ) is not None
    )


def get_tz_aware_datetime(datetime_obj: datetime):
    if not is_datetime_tz_aware(datetime_obj):
        datetime_obj = datetime_obj.replace(tzinfo=pytz.UTC)
    return datetime_obj
