import re
import datetime
from datetime import timezone
from datetime import timedelta
from typing import Optional, Tuple, cast
import logging
# pylint: disable=import-error
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error
)

# pylint: disable=too-few-public-methods
from data_pipeline.utils.web_api import requests_retry_session

LOGGER = logging.getLogger(__name__)


class EtlModuleConstant:
    DEFAULT_DATA_COLLECTION_START_DATE = "2000-01-01"
    # config for the crossref data
    CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY = "timestamp"
    CROSSREF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    MESSAGE_NEXT_CURSOR_KEY = "next-cursor"
    # config for bigquery schema
    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"
    # date format used for y application for maintaining download state
    STATE_FILE_DATE_FORMAT = "%Y-%m-%d"


def get_date_of_days_before_as_string(number_of_days_before: int) -> str:
    dtobj = (
        datetime.datetime.now(timezone.utc) -
        timedelta(number_of_days_before)
    )
    return dtobj.strftime(EtlModuleConstant.STATE_FILE_DATE_FORMAT)


def convert_datetime_to_date_string(
        datetime_obj: datetime.datetime,
        time_format: str = EtlModuleConstant.STATE_FILE_DATE_FORMAT
) -> str:

    return datetime_obj.strftime(time_format)


def parse_datetime_from_str(
        date_as_string: str,
        time_format: str = EtlModuleConstant.STATE_FILE_DATE_FORMAT
):

    return datetime.datetime.strptime(date_as_string.strip(), time_format)


# pylint: disable=broad-except,no-else-return
def get_new_data_download_start_date_from_cloud_storage(
        bucket: str,
        object_key: str,
        no_of_prior_days_to_last_data_collected_date: int = 0
) -> dict:
    try:
        journal_last_record_date = cast(
            # Temporarily casting to avoid linting error
            # This currently can't work properly because the function returns a string
            dict,
            download_s3_object_as_string_or_file_not_found_error(
                bucket, object_key
            )
        )
    except FileNotFoundError:
        LOGGER.info(
            'state file not found, starting with initial state: s3:%s/%s',
            bucket, object_key
        )
        journal_last_record_date = {}
    for journal in journal_last_record_date:
        journal_last_record_date[journal] = (
            get_new_journal_download_start_date_as_str(
                journal_last_record_date.get(journal),
                no_of_prior_days_to_last_data_collected_date,
            )
        )
    return journal_last_record_date


# pylint: disable=broad-except,no-else-return
def get_new_journal_download_start_date_as_str(
        date_as_string, number_of_previous_day_to_process=0
) -> str:
    dtobj = parse_datetime_from_str(date_as_string) - timedelta(
        number_of_previous_day_to_process
    )
    return convert_datetime_to_date_string(dtobj)


# pylint: disable=fixme,too-many-arguments
def get_crossref_data_single_page(
        base_crossref_url: str,
        journal_doi_prefix: str,
        from_date_collected_as_string: str,
        until_collected_date_as_string: Optional[str] = None,
        cursor=None,
        message_key: str = "message",
) -> Tuple[str, dict]:
    # TODO : specify all static url parameter via config
    LOGGER.info('base_crossref_url: %s', base_crossref_url)
    url = (
        base_crossref_url
        + "&from-collected-date="
        + from_date_collected_as_string
        + "&obj-id.prefix="
        + journal_doi_prefix
    )
    LOGGER.info('url: %s', url)
    if until_collected_date_as_string:
        url += "&until-collected-date=" + until_collected_date_as_string
    if cursor:
        url += "&cursor=" + cursor
    with requests_retry_session() as session:
        response = session.get(url)
        try:
            response.raise_for_status()
            resp = response.json()
        except Exception:
            LOGGER.error(
                'Failed to process url: %s | response_status_code: %s | response: %r ',
                url, response.status_code, response.text
            )
            raise
    return resp[message_key][EtlModuleConstant.MESSAGE_NEXT_CURSOR_KEY], resp


def convert_bq_schema_field_list_to_dict(json_list,) -> dict:
    return {
        bq_schema_field.get(EtlModuleConstant.BQ_SCHEMA_FIELD_NAME_KEY):
            bq_schema_field
        for bq_schema_field in json_list
    }


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)
