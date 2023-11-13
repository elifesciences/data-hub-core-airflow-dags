from typing import Mapping, Sequence

from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import (
    BigQueryIncludeExcludeSourceConfigDict,
    BigQueryTargetConfigDict,
    MappingConfigDict
)


SemanticScholarMatrixConfigDict = Mapping[str, BigQueryIncludeExcludeSourceConfigDict]


class SemanticScholarSourceConfigDict(TypedDict):
    apiUrl: str
    params: Mapping[str, str]
    headers: MappingConfigDict


class SemanticScholarItemConfigDict(TypedDict):
    matrix: SemanticScholarMatrixConfigDict
    source: SemanticScholarSourceConfigDict
    target: BigQueryTargetConfigDict
    batchSize: NotRequired[int]


class SemanticScholarConfigDict(TypedDict):
    semanticScholar: Sequence[SemanticScholarItemConfigDict]
