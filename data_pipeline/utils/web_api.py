from dataclasses import dataclass
from typing import Collection, Optional, Sequence

import requests
from requests.adapters import HTTPAdapter

from urllib3.util.retry import Retry


DEFAULT_MAX_RETRY_COUNT = 10
DEFAULT_RETRY_BACKOFF_FACTOR = 0.3
DEFAULT_RETRY_ON_RESPONSE_STATUS_LIST = (500, 502, 504)


@dataclass(frozen=True)
class WebApiRetryConfig:
    max_retry_count: int = DEFAULT_MAX_RETRY_COUNT
    retry_backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR
    retry_on_response_status_list: Sequence[int] = DEFAULT_RETRY_ON_RESPONSE_STATUS_LIST


DEFAULT_WEB_API_RETRY_CONFIG = WebApiRetryConfig()

DISABLED_WEB_API_RETRY_CONFIG = WebApiRetryConfig(
    max_retry_count=0,
    retry_on_response_status_list=tuple([])
)


def requests_retry_session(
    retries: int = DEFAULT_MAX_RETRY_COUNT,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    status_forcelist: Optional[Collection[int]] = DEFAULT_RETRY_ON_RESPONSE_STATUS_LIST,
    session: Optional[requests.Session] = None,
    **kwargs
) -> requests.Session:
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        **kwargs
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def requests_retry_session_for_config(
    config: WebApiRetryConfig,
    session: Optional[requests.Session] = None,
    **kwargs
) -> requests.Session:
    return requests_retry_session(
        retries=config.max_retry_count,
        backoff_factor=config.retry_backoff_factor,
        status_forcelist=config.retry_on_response_status_list,
        session=session,
        **kwargs
    )
