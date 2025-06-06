from typing_extensions import TypedDict, NotRequired, Sequence

from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict


class ScheduledBigQueryConfigDict(TypedDict):
    projectName: str
    sqlQuery: str


class ScheduledQueryPipelineInitialStateConfigDict(TypedDict):
    startDate: str


class ScheduledQueryPipelineStateConfigDict(TypedDict):
    initialState: ScheduledQueryPipelineInitialStateConfigDict
    stateFile: StateFileConfigDict


class ScheduledQueryPipelineConfigDict(TypedDict):
    dataPipelineId: str
    state: NotRequired[ScheduledQueryPipelineStateConfigDict]
    bigQuery: ScheduledBigQueryConfigDict


class MultiScheduledQueryPipelineConfigDict(TypedDict):
    scheduledQueries: Sequence[ScheduledQueryPipelineConfigDict]
