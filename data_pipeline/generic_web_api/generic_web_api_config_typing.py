from typing import Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    BigQueryIncludeExcludeSourceConfigDict,
    MappingConfigDict,
    ParameterFromFileConfigDict,
    StateFileConfigDict
)


class ParameterFromEnvConfigDict(TypedDict):
    parameterName: str
    envName: str


class WebApiConfigurableParametersConfigDict(TypedDict):
    pageParameterName: NotRequired[str]
    offsetParameterName: NotRequired[str]
    pageSizeParameterName: NotRequired[str]
    resultSortParameterName: NotRequired[str]
    resultSortParameterValue: NotRequired[str]
    fromDateParameterName: NotRequired[str]
    toDateParameterName: NotRequired[str]
    dateFormat: NotRequired[str]
    nextPageCursorParameterName: NotRequired[str]
    defaultPageSize: NotRequired[int]
    defaultStartDate: NotRequired[str]
    daysDiffFromStartTillEnd: NotRequired[int]


class WebApiDataUrlConfigDict(TypedDict):
    urlExcludingConfigurableParameters: str
    configurableParameters: NotRequired[WebApiConfigurableParametersConfigDict]
    parametersFromEnv: NotRequired[Sequence[ParameterFromEnvConfigDict]]  # Note: not used anymore
    parametersFromFile: NotRequired[Sequence[ParameterFromFileConfigDict]]


class SchemaFileConfigDict(TypedDict):
    bucketName: str
    objectName: str


class WebApiAuthenticationValueConfigDict(TypedDict):
    value: NotRequired[str]
    envVariableHoldingAuthValue: NotRequired[str]
    envVariableContainingPathToAuthFile: NotRequired[str]


class WebApiAuthenticationConfigDict(TypedDict):
    auth_type: NotRequired[str]
    orderedAuthenticationParamValues: NotRequired[Sequence[WebApiAuthenticationValueConfigDict]]


class WebApiUrlSourceTypeConfigDict(TypedDict):
    name: str
    sourceTypeSpecificValues: NotRequired[dict]


class WebApiRecordTimestampResponseConfigDict(TypedDict):
    itemTimestampKeyFromItemRoot: NotRequired[Sequence[str]]


class WebApiResponseConfigDict(TypedDict):
    itemsKeyFromResponseRoot: NotRequired[Sequence[str]]
    totalItemsCountKeyFromResponseRoot: NotRequired[Sequence[str]]
    nextPageCursorKeyFromResponseRoot: NotRequired[Sequence[str]]
    recordTimestamp: NotRequired[WebApiRecordTimestampResponseConfigDict]


class WebApiBaseConfigDict(TypedDict):
    dataPipelineId: NotRequired[str]
    dataset: str
    table: str
    dataUrl: WebApiDataUrlConfigDict
    authentication: NotRequired[WebApiAuthenticationConfigDict]
    headers: NotRequired[MappingConfigDict]
    urlSourceType: NotRequired[WebApiUrlSourceTypeConfigDict]
    response: NotRequired[WebApiResponseConfigDict]
    source: NotRequired[BigQueryIncludeExcludeSourceConfigDict]
    schemaFile: NotRequired[SchemaFileConfigDict]
    stateFile: NotRequired[StateFileConfigDict]
    batchSize: NotRequired[int]


class WebApiGlobalConfigDict(TypedDict):
    gcpProjectName: str
    importedTimestampFieldName: str


class WebApiConfigDict(WebApiBaseConfigDict, WebApiGlobalConfigDict):
    pass


class MultiWebApiConfigDict(WebApiGlobalConfigDict):
    webApi: Sequence[WebApiBaseConfigDict]
