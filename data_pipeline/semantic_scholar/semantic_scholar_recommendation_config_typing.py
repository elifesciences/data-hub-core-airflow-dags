from typing import Sequence
from typing_extensions import TypedDict

from data_pipeline.semantic_scholar.semantic_scholar_config_typing import (
    SemanticScholarItemConfigDict
)


class SemanticScholarRecommendationConfigDict(TypedDict):
    semanticScholarRecommendation: Sequence[SemanticScholarItemConfigDict]
