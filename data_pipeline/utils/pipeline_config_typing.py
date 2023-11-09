from typing_extensions import NotRequired, TypedDict


class BigQuerySourceConfigDict(TypedDict):
    projectName: str
    sqlQuery: str
    ignoreNotFound: NotRequired[bool]


class BigQueryWrappedSourceConfigDict(TypedDict):
    bigQuery: BigQuerySourceConfigDict


class BigQueryWrappedExcludeSourceConfigDict(BigQueryWrappedSourceConfigDict):
    keyFieldName: str


class BigQueryIncludeExcludeSourceConfigDict(TypedDict):
    include: BigQueryWrappedSourceConfigDict
    exclude: NotRequired[BigQueryWrappedExcludeSourceConfigDict]


class BigQueryTargetConfigDict(TypedDict):
    projectName: str
    datasetName: str
    tableName: str


class StateFileConfigDict(TypedDict):
    bucketName: str
    objectName: str
