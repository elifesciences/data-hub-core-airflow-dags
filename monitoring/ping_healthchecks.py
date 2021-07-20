import logging
import socket
import requests

LOGGER = logging.getLogger(__name__)

# Schedule of healthchecks.io
    # Period (Expected time between pings):	3 hours
    # Grace Time (When a check is late, how long to wait until an alert is sent): 1 hour
HEALTH_CHECKS_URL = "https://hc-ping.com/3549bc53-4bd9-4ef4-9c4a-22aa57c2fb5b"
# as the url shoul not be public what can be the option for the make it private?


def ping():
    LOGGER.info('[healthcheck] pinging alive status...')
    try:
        requests.post(HEALTH_CHECKS_URL)
    except socket.error as err:
        LOGGER.info("Ping failed: %s", err)

    LOGGER.info('[healthcheck] pinging is successful')
