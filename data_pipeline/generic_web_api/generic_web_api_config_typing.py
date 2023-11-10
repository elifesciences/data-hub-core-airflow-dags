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


class WebApiDataUrlConfigDict(TypedDict):
    urlExcludingConfigurableParameters: str
    configurableParameters: NotRequired[dict]
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


class WebApiBaseConfigDict(TypedDict):
    dataPipelineId: NotRequired[str]
    dataset: str
    table: str
    dataUrl: WebApiDataUrlConfigDict
    authentication: NotRequired[WebApiAuthenticationConfigDict]
    headers: NotRequired[MappingConfigDict]
    urlSourceType: NotRequired[dict]
    response: NotRequired[dict]
    source: NotRequired[BigQueryIncludeExcludeSourceConfigDict]
    schemaFile: NotRequired[SchemaFileConfigDict]
    stateFile: NotRequired[StateFileConfigDict]


class WebApiGlobalConfigDict(TypedDict):
    gcpProjectName: str
    importedTimestampFieldName: str


class WebApiConfigDict(WebApiBaseConfigDict, WebApiGlobalConfigDict):
    pass


class MultiWebApiConfigDict(WebApiGlobalConfigDict):
    webApi: Sequence[WebApiBaseConfigDict]
