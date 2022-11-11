import re
import json
import datetime
from datetime import timezone
from datetime import timedelta
from typing import Iterable, Optional, Tuple
import logging
# pylint: disable=import-error
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object
)
from data_pipeline.utils.pipeline_file_io import (
    iter_write_jsonl_to_file
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
        datetime_obj: datetime,
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
    journal_last_record_date = download_s3_json_object(bucket, object_key)
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
        cursor=None,
        journal_doi_prefix: Optional[str] = None,
        from_date_collected_as_string: Optional[str] = None,
        until_collected_date_as_string: Optional[str] = None,
        message_key: str = "message",
) -> Tuple(str, dict):
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


def preprocess_json_record(
        json_list,
        data_hub_imported_timestamp_key,
        data_hub_imported_timestamp, schema
) -> Iterable[dict]:
    return (
        transform_record(
            record,
            data_hub_imported_timestamp_key,
            data_hub_imported_timestamp,
            schema=schema,
        )
        for record in json_list
    )


def get_latest_json_record_list_timestamp(
        json_list, previous_latest_timestamp
):
    latest_collected_record_timestamp_list = [
        parse_datetime_from_str(
            record.get(
                EtlModuleConstant.CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY
            ),
            EtlModuleConstant.CROSSREF_TIMESTAMP_FORMAT,
        )
        for record in json_list
        if record.get(EtlModuleConstant.CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY)
    ]
    latest_collected_record_timestamp_list.append(previous_latest_timestamp)

    return max(latest_collected_record_timestamp_list)


def convert_bq_schema_field_list_to_dict(json_list,) -> dict:
    return {
        bq_schema_field.get(EtlModuleConstant.BQ_SCHEMA_FIELD_NAME_KEY):
            bq_schema_field
        for bq_schema_field in json_list
    }


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def semi_clean_crossref_record(record, schema):
    if isinstance(record, dict):
        list_as_p_dict = convert_bq_schema_field_list_to_dict(schema)
        key_list = set(list_as_p_dict.keys())
        new_dict = {}
        for record_item_key, record_item_val in record.items():
            new_key = standardize_field_name(record_item_key)
            if new_key in key_list:
                if isinstance(record_item_val, (list, dict)):
                    record_item_val = semi_clean_crossref_record(
                        record_item_val,
                        list_as_p_dict.get(new_key).get(
                            EtlModuleConstant.BQ_SCHEMA_SUBFIELD_KEY
                        ),
                    )
                if (
                        list_as_p_dict.get(new_key)
                        .get(EtlModuleConstant.BQ_SCHEMA_FIELD_TYPE_KEY)
                        .lower() == "timestamp"
                ):
                    try:
                        parse_datetime_from_str(
                            record_item_val,
                            EtlModuleConstant.CROSSREF_TIMESTAMP_FORMAT
                        )
                    except BaseException:
                        record_item_val = None
                new_dict[new_key] = record_item_val
        return new_dict
    elif isinstance(record, list):
        new_list = []
        for elem in record:
            if isinstance(elem, (dict, list)):
                elem = semi_clean_crossref_record(elem, schema)
            if elem is not None:
                new_list.append(elem)
        return new_list


def transform_record(
        record, imported_timestamp_key, imported_timestamp, schema
) -> dict:
    new_record = semi_clean_crossref_record(record, schema)
    new_record[imported_timestamp_key] = imported_timestamp
    return new_record


# pylint: disable=too-many-arguments,too-many-locals
def per_doi_download_page_etl(
        base_crossref_url: str,
        from_date_collected_as_string: str,
        until_collected_date_as_string: str,
        journal_doi_prefix: str,
        cursor: str,
        message_key: str,
        event_key: str,
        imported_timestamp_key: str,
        imported_timestamp: datetime,
        full_temp_file_location: str,
        schema: list,
        journal_previous_timestamp: datetime,
):
    cursor, downloaded_data = get_crossref_data_single_page(
        base_crossref_url=base_crossref_url,
        cursor=cursor,
        from_date_collected_as_string=from_date_collected_as_string,
        journal_doi_prefix=journal_doi_prefix,
        message_key=message_key,
        until_collected_date_as_string=until_collected_date_as_string,
    )
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    n_results = preprocess_json_record(
        results, imported_timestamp_key, imported_timestamp, schema
    )
    written_json_record = iter_write_jsonl_to_file(
        n_results, full_temp_file_location
    )

    latest_collected_record_timestamp = get_latest_json_record_list_timestamp(
        written_json_record, journal_previous_timestamp
    )
    return latest_collected_record_timestamp, cursor


# pylint: disable=too-many-arguments,too-many-locals
def etl_crossref_data_single_journal_return_latest_timestamp(
        base_crossref_url: str,
        from_date_as_string: str,
        journal_doi_prefix: str,
        message_key: str,
        event_key: str,
        imported_timestamp_key: str,
        imported_timestamp,
        full_temp_file_location: str,
        schema: list,
        until_date_as_string: Optional[str] = None,
) -> datetime:
    latest_collected_record_timestamp = parse_datetime_from_str(
        from_date_as_string
    )
    journal_latest_timestamp = latest_collected_record_timestamp
    cursor = None
    while True:
        journal_latest_timestamp, cursor = per_doi_download_page_etl(
            base_crossref_url=base_crossref_url,
            from_date_collected_as_string=from_date_as_string,
            until_collected_date_as_string=until_date_as_string,
            journal_doi_prefix=journal_doi_prefix,
            cursor=cursor,
            message_key=message_key,
            event_key=event_key,
            imported_timestamp_key=imported_timestamp_key,
            imported_timestamp=imported_timestamp,
            full_temp_file_location=full_temp_file_location,
            schema=schema,
            journal_previous_timestamp=journal_latest_timestamp,
        )
        if not cursor:
            break
    return journal_latest_timestamp


# pylint: disable=too-many-arguments,too-many-locals
def etl_crossref_data_return_latest_timestamp(
        base_crossref_url: str,
        latest_journal_download_date: dict,
        journal_doi_prefixes: list,
        message_key: str,
        event_key: str,
        imported_timestamp_key: str,
        imported_timestamp,
        full_temp_file_location: str,
        schema: list,
        until_date_as_string: Optional[str] = None,
) -> str:

    journal_latest_timestamp = {}
    for journal_doi_prefix in journal_doi_prefixes:
        from_date_as_string = latest_journal_download_date.get(
            journal_doi_prefix,
            EtlModuleConstant.DEFAULT_DATA_COLLECTION_START_DATE
        )
        latest_timestamp = (
            etl_crossref_data_single_journal_return_latest_timestamp(
                base_crossref_url,
                from_date_as_string,
                journal_doi_prefix,
                message_key,
                event_key,
                imported_timestamp_key,
                imported_timestamp,
                full_temp_file_location,
                schema,
                until_date_as_string,
            )
        )

        journal_latest_timestamp[journal_doi_prefix] = (
            convert_datetime_to_date_string(
                latest_timestamp
            )
        )

    return json.dumps(
        journal_latest_timestamp, ensure_ascii=False, indent=4
    )


def add_data_hub_timestamp_field_to_bigquery_schema(
        schema_json, imported_timestamp_field_name
) -> list:
    new_schema = [
        x for x in schema_json if imported_timestamp_field_name not in x.keys()
    ]
    new_schema.append(
        {
            "mode": "NULLABLE",
            "name": imported_timestamp_field_name,
            "type": "TIMESTAMP",
        }
    )
    return new_schema
