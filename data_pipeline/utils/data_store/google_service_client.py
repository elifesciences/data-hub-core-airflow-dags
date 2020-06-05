import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery_cache.base import Cache

LOGGER = logging.getLogger(__name__)


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


def get_gcp_cred_file_location():
    return os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


# pylint: disable=no-member
def get_credentials(scopes):
    g_cred_loc = get_gcp_cred_file_location()
    try:
        credentials = service_account.Credentials.from_service_account_file(
            g_cred_loc, scopes=scopes
        )
        return credentials
    except OSError as err:
        LOGGER.error(
            "Google application credentials file not found at  %s : %s",
            g_cred_loc, err,
        )
        raise
