from typing_extensions import NotRequired, TypedDict


class BigQuerySourceConfigDict(TypedDict):
    projectName: str
    sqlQuery: str
    ignoreNotFound: NotRequired[bool]


class BigQueryWrappedSourceConfigDict(TypedDict):
    bigQuery: BigQuerySourceConfigDict


class BigQueryWrappedExcludeSourceConfigDict(BigQueryWrappedSourceConfigDict):
    keyFieldName: str
