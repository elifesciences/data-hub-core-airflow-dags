import json
import datetime
import logging
from typing import Iterable, Sequence
import requests

LOGGER = logging.getLogger(__name__)


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


def get_mailing_id_dict(mail_id: int) -> str(dict):
    return json.dumps({'mailing_id': mail_id})


def get_connection_parameters(mail_id: int, api_key, site_key, is_distinct=0):
    return {
        "entity": "Mailing",
        "action": "stats",
        "json": get_mailing_id_dict(mail_id),
        "api_key": api_key,
        "key": site_key,
        "is_distinct": is_distinct
    }


def connect_civiapi_and_get_email_report(
    url: str,
    mail_id: int,
    api_key: str,
    site_key: str,
    is_distinct: int
) -> dict:
    response = requests.post(
        url=url,
        data=get_connection_parameters(mail_id, api_key, site_key, is_distinct)
    )
    response.raise_for_status()
    return response.json()


def combine_non_distinct_and_distinct_value_email_reports(
    url: str,
    mail_id: int,
    api_key: str,
    site_key: str
) -> dict:
    report = {}
    report["report_with_non_distinct_values"] = connect_civiapi_and_get_email_report(
        url=url,
        mail_id=mail_id,
        api_key=api_key,
        site_key=site_key,
        is_distinct=0
    )
    report["report_with_distinct_values"] = connect_civiapi_and_get_email_report(
        url=url,
        mail_id=mail_id,
        api_key=api_key,
        site_key=site_key,
        is_distinct=1
    )
    return report


def transform_email_report(
    dict_response: dict,
    mail_id: int
) -> dict:
    mail_id_str = str(mail_id)
    return {
        "mail_id": mail_id,
        "successful_deliveries": int(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["Delivered"]),
        "bounces": int(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["Bounces"]),
        "unsubscribe_requests": int(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["Unsubscribers"]),
        "total_clicks": int(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["Unique Clicks"]),
        "unique_clicks": int(dict_response[
            "report_with_distinct_values"
        ]["values"][mail_id_str]["Unique Clicks"]),
        "total_opens": int(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["Opened"]),
        "unique_opens": int(dict_response[
            "report_with_distinct_values"
        ]["values"][mail_id_str]["Opened"]),
        "opened_rate": float(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["opened_rate"].replace("%", "")),
        "delivered_rate": float(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["delivered_rate"].replace("%", "")),
        "clickthrough_rate": float(dict_response[
            "report_with_non_distinct_values"
        ]["values"][mail_id_str]["clickthrough_rate"].replace("%", "")),
        "imported_timestamp": get_current_timestamp()
    }


def iter_email_report(
    url: str,
    mail_id_list: Sequence[str],
    api_key: str,
    site_key: str
) -> Iterable[dict]:
    LOGGER.info("total email count is %s", len(mail_id_list))
    for mail_id in mail_id_list:
        LOGGER.info("mail_id to process is %s", mail_id)
        yield transform_email_report(
            dict_response=combine_non_distinct_and_distinct_value_email_reports(
                url=url,
                mail_id=mail_id,
                api_key=api_key,
                site_key=site_key
            ),
            mail_id=mail_id
        )
