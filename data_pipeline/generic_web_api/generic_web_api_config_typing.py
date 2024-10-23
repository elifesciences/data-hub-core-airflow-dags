from typing import Literal, Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    AirflowConfigDict,
    BigQueryIncludeExcludeSourceConfigDict,
    MappingConfigDict,
    ParameterFromFileConfigDict,
    RecordProcessingStepConfigList,
    StateFileConfigDict
)
from data_pipeline.utils.web_api_typing import WebApiRetryConfigDict


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


class WebApiRequestBuilderConfigDict(TypedDict):
    name: str
    parameters: NotRequired[dict]
    sourceTypeSpecificValues: NotRequired[dict]  # deprecated


class WebApiRecordTimestampResponseConfigDict(TypedDict):
    itemTimestampKeyFromItemRoot: NotRequired[Sequence[str]]


OnSameNextCursorConfig = Literal['Error', 'Stop', 'Continue']


class WebApiResponseConfigDict(TypedDict):
    itemsKeyFromResponseRoot: NotRequired[Sequence[str]]
    totalItemsCountKeyFromResponseRoot: NotRequired[Sequence[str]]
    nextPageCursorKeyFromResponseRoot: NotRequired[Sequence[str]]
    recordTimestamp: NotRequired[WebApiRecordTimestampResponseConfigDict]
    fieldsToReturn: NotRequired[Sequence[str]]
    recordProcessingSteps: NotRequired[RecordProcessingStepConfigList]
    provenanceEnabled: NotRequired[bool]
    onSameNextCursor: NotRequired[OnSameNextCursorConfig]


class WebApiBaseConfigDict(TypedDict):
    dataPipelineId: str
    airflow: NotRequired[AirflowConfigDict]
    description: NotRequired[str]
    dataset: str
    table: str
    dataUrl: WebApiDataUrlConfigDict
    authentication: NotRequired[WebApiAuthenticationConfigDict]
    headers: NotRequired[MappingConfigDict]
    retry: NotRequired[WebApiRetryConfigDict]
    requestBuilder: NotRequired[WebApiRequestBuilderConfigDict]
    urlSourceType: NotRequired[WebApiRequestBuilderConfigDict]  # deprecated
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


class MultiWebApiDefaultConfigDict(TypedDict):
    airflow: NotRequired[AirflowConfigDict]


class MultiWebApiConfigDict(WebApiGlobalConfigDict):
    defaultConfig: NotRequired[MultiWebApiDefaultConfigDict]
    webApi: Sequence[WebApiBaseConfigDict]
