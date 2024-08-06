from typing_extensions import TypedDict


class OpenSearchIngestionPipelineConfigDict(TypedDict):
    name: str
    definition: str
