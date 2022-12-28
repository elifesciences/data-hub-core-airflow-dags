from datetime import (
    datetime,
    timezone,
    date,
    timedelta
)
from typing import Optional
import dateparser
import pytz


def get_current_timestamp() -> datetime:
    return datetime.now(timezone.utc)


def get_current_timestamp_as_string(
        time_format: str = "%Y-%m-%dT%H:%M:%SZ"
):
    return get_current_timestamp().strftime(time_format)


def datetime_to_string(
        datetime_obj: Optional[datetime] = None,
        datetime_format: Optional[str] = None
) -> Optional[str]:
    return datetime_obj.strftime(datetime_format) if datetime_obj and datetime_format else None


def parse_timestamp_from_str(timestamp_as_str, time_format: Optional[str] = None):
    if time_format:
        timestamp_obj: Optional[datetime] = datetime.strptime(
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


def get_yesterdays_date() -> date:
    today = date.today()
    return today - timedelta(days=1)


def get_todays_date() -> date:
    return date.today()
