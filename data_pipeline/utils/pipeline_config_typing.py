from typing import Any, Mapping
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


class ParameterFromFileConfigDict(TypedDict):
    parameterName: str
    filePathEnvName: str


# Note: MappingConfigDict may contain `parametersFromFile` of type
#   `Sequence[ParameterFromFileConfigDict]`
#   all other keys should have a value type `str`
#   It seems difficult to express.
#   Consider moving other keys into `values`.
MappingConfigDict = Mapping[str, Any]
