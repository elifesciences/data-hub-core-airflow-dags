import os
import logging

from apiclient import discovery
from google.oauth2 import service_account
from googleapiclient.discovery_cache.base import Cache
from googleapiclient.errors import HttpError


LOGGER = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_DATA_KEY = "values"

def get_gcp_cred_file_location():
    return os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


def download_google_spreadsheet_single_sheet(spreadsheet_id: str, sheet_range: str):
    g_cred_loc = get_gcp_cred_file_location()
    try:
        credentials = service_account.Credentials.from_service_account_file(
            g_cred_loc, scopes=SCOPES
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
    except OSError as e:
        LOGGER.error(
            "Google application credentials file not found at  %s : %s",
            g_cred_loc,
            e,
        )
    return []



