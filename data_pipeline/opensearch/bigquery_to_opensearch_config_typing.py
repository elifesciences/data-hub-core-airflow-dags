from typing import Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    ParameterFromFileConfigDict
)


class OpenSearchSecretsConfigDict(TypedDict):
    parametersFromFile: Sequence[ParameterFromFileConfigDict]


class OpenSearchIngestionPipelineConfigDict(TypedDict):
    name: str
    definition: str


class OpenSearchTargetConfigDict(TypedDict):
    hostname: str
    port: int
    secrets: OpenSearchSecretsConfigDict
    indexName: str
    timeout: NotRequired[float]
    updateIndexSettings: NotRequired[bool]
    updateMappings: NotRequired[bool]
    ingestionPipelines: NotRequired[Sequence[OpenSearchIngestionPipelineConfigDict]]
    indexSettings: NotRequired[dict]
    verifyCertificates: NotRequired[bool]
    operationMode: NotRequired[str]
    upsert: NotRequired[bool]
