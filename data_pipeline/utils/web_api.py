from typing import Iterable, Optional
import requests
from requests.adapters import HTTPAdapter

from urllib3.util.retry import Retry


def requests_retry_session(
    retries: int = 10,
    backoff_factor: float = 0.3,
    status_forcelist: Iterable[int] = (500, 502, 504),
    session: Optional[requests.Session] = None,
) -> requests.Session:
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
