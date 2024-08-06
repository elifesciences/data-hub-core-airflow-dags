from typing import Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    ParameterFromFileConfigDict
)


class OpenSearchIngestionPipelineConfigDict(TypedDict):
    name: str
    definition: str


class OpenSearchSecretsConfigDict(TypedDict):
    parametersFromFile: Sequence[ParameterFromFileConfigDict]


class OpenSearchTargetConfigDict(TypedDict):
    hostname: str
    port: int
    secrets: OpenSearchSecretsConfigDict
    indexName: str
    timeout: NotRequired[float]
    updateIndexSettings: NotRequired[bool]
    updateMappings: NotRequired[bool]
    indexSettings: NotRequired[dict]
    verifyCertificates: NotRequired[bool]
    operationMode: NotRequired[str]
    upsert: NotRequired[bool]
