import json
import datetime
import logging
from typing import Iterable
import requests

LOGGER = logging.getLogger(__name__)


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


def get_mailing_id_dict(mail_id: int) -> str(dict):
    return json.dumps({'mailing_id': mail_id})


def get_connection_parameters(mail_id: int, api_key, site_key):
    return {
        "entity": "Mailing",
        "action": "stats",
        "json": get_mailing_id_dict(mail_id),
        "api_key": api_key,
        "key": site_key
    }


def get_email_report(
    url: str,
    mail_id: int,
    api_key: str,
    site_key: str
) -> dict:
    response = requests.post(url=url, data=get_connection_parameters(mail_id, api_key, site_key))
    response.raise_for_status()
    dict_response = eval(response.text)
    mail_id_str = str(mail_id)
    return {
        "mail_id": mail_id,
        "delivered": dict_response["values"][mail_id_str]["Delivered"],
        "delivered_rate": dict_response["values"][mail_id_str]["delivered_rate"],
        "bounces": dict_response["values"][mail_id_str]["Bounces"],
        "unsubscribers": dict_response["values"][mail_id_str]["Unsubscribers"],
        "unique_clicks": dict_response["values"][mail_id_str]["Unique Clicks"],
        "opened": dict_response["values"][mail_id_str]["Opened"],
        "opened_rate": dict_response["values"][mail_id_str]["opened_rate"],
        "clickthrough_rate": dict_response["values"][mail_id_str]["clickthrough_rate"],
        "imported_timestamp": get_current_timestamp()
    }


def iter_email_report(
    url: str,
    mail_id_list: list,
    api_key: str,
    site_key: str
) -> Iterable[dict]:
    LOGGER.info("total email count is %s", len(mail_id_list))
    for mail_id in mail_id_list:
        LOGGER.info("mail_id to process is %s", mail_id[1])
        yield get_email_report(url, int(mail_id[1]), api_key, site_key)
