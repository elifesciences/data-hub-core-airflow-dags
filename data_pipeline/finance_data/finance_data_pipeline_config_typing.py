from typing import TypedDict


class FinanceDataSourceConfigDict(TypedDict):
    projectName: str
    dataset: str
    table: str


class FinanceDataTargetConfigDict(TypedDict):
    bucket: str
    objectName: str


class FinanceDataPipelineConfigDict(TypedDict):
    dataPipelineId: str
    source: FinanceDataSourceConfigDict
    target: FinanceDataTargetConfigDict
