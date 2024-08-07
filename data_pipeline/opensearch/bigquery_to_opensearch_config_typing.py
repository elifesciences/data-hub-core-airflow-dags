from typing import Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    ParameterFromFileConfigDict
)


class OpenSearchSecretsConfigDict(TypedDict):
    parametersFromFile: Sequence[ParameterFromFileConfigDict]


class OpenSearchIngestPipelineTestConfigDict(TypedDict):
    description: str
    inputDocument: str
    expectedDocument: str


class OpenSearchIngestPipelineConfigDict(TypedDict):
    name: str
    definition: str
    tests: NotRequired[Sequence[OpenSearchIngestPipelineTestConfigDict]]


class OpenSearchTargetConfigDict(TypedDict):
    hostname: str
    port: int
    secrets: OpenSearchSecretsConfigDict
    indexName: str
    timeout: NotRequired[float]
    updateIndexSettings: NotRequired[bool]
    updateMappings: NotRequired[bool]
    ingestPipelines: NotRequired[Sequence[OpenSearchIngestPipelineConfigDict]]
    indexSettings: NotRequired[dict]
    verifyCertificates: NotRequired[bool]
    operationMode: NotRequired[str]
    upsert: NotRequired[bool]
