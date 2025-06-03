from typing_extensions import TypedDict, NotRequired, Sequence

from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict


class ScheduledBigQueryConfigDict(TypedDict):
    projectName: str
    sqlQuery: str


class ScheduledQueryPipelineConfigDict(TypedDict):
    dataPipelineId: str
    stateFile: NotRequired[StateFileConfigDict]
    bigQuery: ScheduledBigQueryConfigDict


class MultiScheduledQueryPipelineConfigDict(TypedDict):
    scheduledQueries: Sequence[ScheduledQueryPipelineConfigDict]
