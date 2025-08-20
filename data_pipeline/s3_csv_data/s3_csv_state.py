from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ObjectPatternCsvState:
    last_modified_timestamp: datetime


@dataclass(frozen=True)
class CsvState:
    state_dict: dict[str, ObjectPatternCsvState]
