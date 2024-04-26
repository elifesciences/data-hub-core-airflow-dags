import logging
from typing import Optional, Sequence
from apiclient import discovery
from data_pipeline.utils.data_store.google_service_client import (
    MemoryCache, get_credentials
)

LOGGER = logging.getLogger(__name__)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

DEFAULT_PAGE_SIZE: int = 5000


class GoogleAnalyticsClient:
    def __init__(self):
        credentials = get_credentials(
            SCOPES
        )
        self.analytics_reporting = discovery.build(
            "analyticsreporting", "v4",
            credentials=credentials, cache=MemoryCache()
        )

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
        return self.analytics_reporting.reports().batchGet(
            body={
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
        ).execute()
