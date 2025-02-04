from typing import Sequence
from typing_extensions import NotRequired, TypedDict


class WebApiRetryConfigDict(TypedDict):
    maxRetryCount: NotRequired[int]
    retryBackoffFactor: NotRequired[float]
    retryOnResponseStatusList: NotRequired[Sequence[int]]
