from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Iterable, Mapping


LOGGER = logging.getLogger(__name__)


DATETIME_FORMAT = r"%Y-%m-%d %H:%M:%S"


def convert_datetime_string_to_datetime(
    datetime_as_string: str
) -> datetime:
    timestamp = datetime.fromisoformat(datetime_as_string)
    if not timestamp.tzinfo:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    LOGGER.debug('parsed timestamp: %r => %r', datetime_as_string, timestamp)
    return timestamp


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

    @staticmethod
    def get_initial_state(
        object_patterns: Iterable[str],
        last_modified_datetime: datetime
    ) -> 'CsvState':
        return CsvState(
            state_dict={
                object_pattern: ObjectPatternCsvState(
                    last_modified_datetime=last_modified_datetime
                )
                for object_pattern in object_patterns
            }
        )

    def to_dict(self) -> Mapping[str, str]:
        return {
            object_pattern: (
                object_pattern_csv_state.last_modified_datetime.isoformat()
            )
            for object_pattern, object_pattern_csv_state in self.state_dict.items()
        }

    def update_last_modified_datetime(
        self,
        object_pattern: str,
        last_modified_datetime: datetime
    ):
        self.state_dict[object_pattern] = ObjectPatternCsvState(
            last_modified_datetime=last_modified_datetime
        )
