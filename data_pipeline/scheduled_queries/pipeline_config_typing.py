from typing import Sequence
from typing_extensions import TypedDict


class ScheduledBigQueryConfigDict(TypedDict):
    projectName: str
    sqlQuery: str


class ScheduledQueryPipelineConfigDict(TypedDict):
    dataPipelineId: str
    bigQuery: ScheduledBigQueryConfigDict


class MultiScheduledQueryPipelineConfigDict(TypedDict):
    scheduledQueries: Sequence[ScheduledQueryPipelineConfigDict]
