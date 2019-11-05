"""
Primarily written to  . . . .
Writes the data primarily to  . . . . .
@author: mowonibi
"""

import requests
import datetime
import os
from datetime import timezone
from datetime import timedelta
from data_pipeline.utils.cloud_data_store.s3_data_service import download_s3_object
import re
import json

CROSSREF_DATA_COLLECTION_BEGINNING = "2000-01-01"
CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY = "timestamp"
CROSSREF_TIMESTAMP_FORMAT="%Y-%m-%dT%H:%M:%SZ"
MESSAGE_NEXT_CURSOR_KEY = "next-cursor"
BQ_SCHEMA_FIELD_NAME_KEY = "name"
BQ_SCHEMA_SUBFIELD_KEY = "fields"
BQ_SCHEMA_FIELD_TYPE_KEY = "type"

def get_date_days_before_as_string(number_of_days_before):
    dtobj = datetime.datetime.now(timezone.utc) - timedelta(number_of_days_before)
    return dtobj.strftime("%Y-%m-%d")


def get_last_run_day_from_cloud_storage(
    bucket: str, object_key: str, number_of_previous_day_to_process=1
):
    try:
        date_as_string = download_s3_object(bucket, object_key)
        pattern = r"^\d\d\d\d-\d\d-\d\d$"
        if (
            date_as_string is not None
            and len(re.findall(pattern, date_as_string.strip())) == 1
        ):
            dtobj = datetime.datetime.strptime(
                date_as_string.strip(), "%Y-%m-%d"
            ) - timedelta(number_of_previous_day_to_process)
            return dtobj.strftime("%Y-%m-%d")
        else:
            return get_date_days_before_as_string(number_of_previous_day_to_process)
    except:
        return CROSSREF_DATA_COLLECTION_BEGINNING


def get_crossref_data_single_page(
    base_crossref_url: str,
    cursor=None,
    publisher_id: str = "",
    from_date_collected_as_string: str = "2019-08-01",
    message_key: str = "message",
):
    # TODO : specify all static url parameter via config
    url = (
        base_crossref_url
        + "&from-collected-date="
        + from_date_collected_as_string
        + "&obj-id.prefix="
        + publisher_id
    )
    if cursor:
        url += "&cursor=" + cursor
    # r = requests.get(url)
    s = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=10)
    b = requests.adapters.HTTPAdapter(max_retries=10)
    s.mount("http://", a)
    s.mount("https://", b)
    r = s.get(url)
    r.raise_for_status()
    resp = r.json()
    return resp[message_key][MESSAGE_NEXT_CURSOR_KEY], resp


def write_result_to_file_get_latest_record_timestamp(
    json_list,
    full_temp_file_location: str,
    previous_latest_timestamp,
    imported_timestamp_key,
    imported_timestamp,
    schema
):
    latest_collected_record_timestamp = previous_latest_timestamp
    with open(full_temp_file_location, "a") as write_file:
        for record in json_list:
            n_record = cleanse_record_add_imported_timestamp_field(
                record, imported_timestamp_key, imported_timestamp, schema=schema
            )
            write_file.write(json.dumps(n_record))
            write_file.write("\n")
            record_collection_timestamp = datetime.datetime.strptime(
                n_record.get(CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY),
                CROSSREF_TIMESTAMP_FORMAT
            )
            latest_collected_record_timestamp = (
                latest_collected_record_timestamp
                if latest_collected_record_timestamp > record_collection_timestamp
                else record_collection_timestamp
            )
    return latest_collected_record_timestamp


def convert_bq_schema_field_list_to_dict(json_list, ):
    k_name = BQ_SCHEMA_FIELD_NAME_KEY.lower()
    schema_list_as_dict = dict()
    for bq_schema_field in json_list:
        bq_field_name_list = [v for k, v in bq_schema_field.items() if k.lower() == k_name]
        bq_schema_field_lower_case_key_name = {k.lower(): v for k, v in bq_schema_field.items()}
        if len(bq_field_name_list) == 1:
            schema_list_as_dict[bq_field_name_list[0]] = bq_schema_field_lower_case_key_name
    return schema_list_as_dict


def standardize_field_name(field_name):
    return re.sub('\W', '_', field_name)


def semi_clean_crossref_record(record, schema):
    if isinstance(record, dict):
        list_as_p_dict = convert_bq_schema_field_list_to_dict(schema)
        key_list = set(list_as_p_dict.keys())
        new_dict = {}
        for k, v in record.items():
            new_key = standardize_field_name(k)
            if new_key in key_list:
                if isinstance(v, dict) or isinstance(v, list):
                    v = semi_clean_crossref_record(v, list_as_p_dict.get(new_key).get(BQ_SCHEMA_SUBFIELD_KEY))
                if list_as_p_dict.get(new_key).get(BQ_SCHEMA_FIELD_TYPE_KEY).lower() == 'timestamp':
                    try:
                        datetime.datetime.strptime(
                            v, CROSSREF_TIMESTAMP_FORMAT
                        )
                    except:
                        v = None
                new_dict[new_key] = v
        return new_dict
        new_list = list()
        for elem in record:
            if isinstance(elem, dict) or isinstance(elem, list):
                elem = semi_clean_crossref_record(elem, schema)
            if elem is not None:
                new_list.append(elem)
        return new_list


def cleanse_record_add_imported_timestamp_field(
    record, imported_timestamp_key, imported_timestamp, schema
):
    new_record = semi_clean_crossref_record(record, schema)
    new_record[imported_timestamp_key] = imported_timestamp
    return new_record


def etl_crossref_data(
    base_crossref_url: str,
    from_date_collected_as_string: str,
    publisher_id: str,
    message_key,
    event_key,
    imported_timestamp_key,
    imported_timestamp,
    full_temp_file_location,
    schema
):
    if os.path.exists(full_temp_file_location):
        os.remove(full_temp_file_location)
    cursor, downloaded_data = get_crossref_data_single_page(
        base_crossref_url=base_crossref_url,
        from_date_collected_as_string=from_date_collected_as_string,
        publisher_id=publisher_id,
        message_key=message_key,
    )
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    latest_collected_record_timestamp = datetime.datetime.strptime(
        "2000-01-01T00:00:00Z", CROSSREF_TIMESTAMP_FORMAT
    )
    latest_collected_record_timestamp = write_result_to_file_get_latest_record_timestamp(
        results,
        full_temp_file_location,
        latest_collected_record_timestamp,
        imported_timestamp_key,
        imported_timestamp,
        schema=schema
    )

    while cursor:
        cursor, downloaded_data = get_crossref_data_single_page(
            base_crossref_url=base_crossref_url,
            cursor=cursor,
            publisher_id=publisher_id,
            from_date_collected_as_string=from_date_collected_as_string,
            message_key=message_key,
        )
        results = downloaded_data.get(message_key, {}).get(event_key, [])
        latest_collected_record_timestamp = write_result_to_file_get_latest_record_timestamp(
            results,
            full_temp_file_location,
            latest_collected_record_timestamp,
            imported_timestamp_key,
            imported_timestamp,
            schema=schema
        )
    return latest_collected_record_timestamp


class CrossRefimportDataPipelineConfig:
    def __init__(self, data_config):
        self.PROJECT_NAME = data_config.get("PROJECT_NAME")
        self.DATASET = data_config.get("DATASET")
        self.TABLE = data_config.get("TABLE")
        self.IMPORTED_TIMESTAMP_FIELD = data_config.get("IMPORTED_TIMESTAMP_FIELD")
        self.SCHEMA_FILE_S3_BUCKET = data_config.get("SCHEMA_FILE").get("BUCKET")
        self.STATE_FILE_NAME_KEY = data_config.get("STATE_FILE").get("OBJECT_NAME")
        self.STATE_FILE_BUCKET = data_config.get("STATE_FILE").get("BUCKET")
        self.TEMP_FILE_DIR = data_config.get("LOCAL_TEMPFILE_DIR")
        self.NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS = data_config.get(
            "NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS"
        )
        self.MESSAGE_KEY = data_config.get("MESSAGE_KEY")
        self.PUBLISHER_ID = data_config.get("PUBLISHER_ID")
        self.CROSSREF_EVENT_BASE_URL = data_config.get("CROSSREF_EVENT_BASE_URL")
        self.EVENT_KEY = data_config.get("EVENT_KEY")
        self.SCHEMA_FILE_OBJECT_NAME = data_config.get("SCHEMA_FILE").get("OBJECT_NAME")
