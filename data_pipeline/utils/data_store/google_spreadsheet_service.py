import logging

from apiclient import discovery
from googleapiclient.errors import HttpError
from data_pipeline.utils.data_store.google_service_client import (
    get_credentials, MemoryCache
)


LOGGER = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_DATA_KEY = "values"


# pylint: disable=no-member
def download_google_spreadsheet_single_sheet(
        spreadsheet_id: str,
        sheet_range: str
):
    credentials = get_credentials(
        SCOPES
    )

    service = discovery.build(
        "sheets", "v4", credentials=credentials, cache=MemoryCache()
    )

    try:
        response = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_range)
            .execute()
        )
        sheets_data = response.get(SHEET_DATA_KEY)
        return sheets_data
    except HttpError as err:
        LOGGER.error("Spreadsheet not downloaded: %s", err.content)
        raise
