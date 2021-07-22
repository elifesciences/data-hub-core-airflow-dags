import os
import logging
import socket
import requests

LOGGER = logging.getLogger(__name__)

HEALTH_CHECK_URL_ENV = "HEALTH_CHECK_URL"
DEFAULT_HEALTH_CHECK_URL= "https://hc-ping.com/3549bc53-4bd9-4ef4-9c4a-22aa57c2fb5b"


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def ping():
    url = get_env_var_or_use_default(
        HEALTH_CHECK_URL_ENV, DEFAULT_HEALTH_CHECK_URL)
    LOGGER.info('[healthcheck] pinging url: %s', url)
    try:
        response = requests.post(url)
        LOGGER.info(response.raise_for_status())
    except socket.error as err:
        LOGGER.info("Ping failed: %s", err)
        LOGGER.info(response.raise_for_status())
