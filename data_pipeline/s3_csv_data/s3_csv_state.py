from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from dateutil import tz


DATETIME_FORMAT = r"%Y-%m-%d %H:%M:%S"


def convert_datetime_string_to_datetime(
    datetime_as_string: str
) -> datetime:
    tz_unaware = datetime.strptime(datetime_as_string.strip(), DATETIME_FORMAT)
    tz_aware = tz_unaware.replace(tzinfo=tz.tzlocal())

    return tz_aware


@dataclass(frozen=True)
class ObjectPatternCsvState:
    last_modified_datetime: datetime


@dataclass(frozen=True)
class CsvState:
    state_dict: dict[str, ObjectPatternCsvState]

    @staticmethod
    def from_dict(state_dict: Mapping[str, str]) -> 'CsvState':
        return CsvState(
            state_dict={
                object_pattern: ObjectPatternCsvState(
                    last_modified_datetime=convert_datetime_string_to_datetime(
                        datetime_str
                    )
                )
                for object_pattern, datetime_str in state_dict.items()
            }
        )
