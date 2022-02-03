import json
import requests
import datetime


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


def get_mailing_id_dict(mail_id: int) -> str(dict):
    return json.dumps({'mailing_id': mail_id})


def get_connection_parameters(mail_id: int, api_key, site_key):
    return {
        "entity": "Mailing",
        "action": "stats",
        "json" : get_mailing_id_dict(mail_id),
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
