import logging
from typing import Optional, Sequence

import google.auth.transport.requests
import requests

from data_pipeline.utils.data_store.google_service_client import (
    get_credentials
)


LOGGER = logging.getLogger(__name__)


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

DEFAULT_PAGE_SIZE: int = 5000

DEFAULT_TIMEOUT: int = 10 * 60


class GoogleAnalyticsClient:
    def __init__(
        self,
        session: requests.Session,
        timeout: int = DEFAULT_TIMEOUT
    ):
        self.credentials = get_credentials(
            SCOPES
        )
        self.session = session
        self.timeout = timeout

    # pylint: disable=too-many-arguments
    def get_report(
        self,
        view_id: str,
        date_ranges: Sequence[dict],
        metrics: Sequence[dict],
        dimensions: Sequence[dict],
        page_token: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE
    ):
        # pylint: disable=no-member
        json_request = {
            'reportRequests': [
                {
                    'viewId': view_id,
                    'pageToken': page_token,
                    'dateRanges': date_ranges,
                    'metrics': metrics,
                    'dimensions': dimensions,
                    'pageSize': page_size
                }
            ]
        }
        self.credentials.refresh(
            google.auth.transport.requests.Request(
                session=self.session
            )
        )
        response = requests.post(
            'https://analyticsreporting.googleapis.com/v4/reports:batchGet',
            json=json_request,
            headers={
                'Authorization': 'Bearer ' + self.credentials.token
            },
            timeout=self.timeout
        )
        return response.json()
