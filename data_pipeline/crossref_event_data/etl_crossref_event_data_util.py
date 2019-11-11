"""
Primarily written to  . . . .
Writes the data primarily to  . . . . .
@author: mowonibi
"""
import os
import re
import json
import datetime
from datetime import timezone
from datetime import timedelta
import requests
from data_pipeline.utils.cloud_data_store.s3_data_service \
    import download_s3_object


# pylint: disable=too-few-public-methods
class ModuleConfig:
    """
    configuration for module
    """
    CROSSREF_DATA_COLLECTION_BEGINNING = "2000-01-01"
    CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY = "timestamp"
    CROSSREF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    MESSAGE_NEXT_CURSOR_KEY = "next-cursor"
    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"
    STATE_FILE_DATE_FORMAT = '%Y-%m-%d'


def get_date_days_before_as_string(number_of_days_before):
    """
    :param number_of_days_before:
    :return:
    """
    dtobj = datetime.datetime.now(
        timezone.utc) - timedelta(number_of_days_before)
    return dtobj.strftime(ModuleConfig.STATE_FILE_DATE_FORMAT)


def convert_latest_data_retrieved_to_string(datetime_obj):
    """
    :param datetime_obj:
    :return:
    """
    return datetime_obj.strftime(ModuleConfig.STATE_FILE_DATE_FORMAT)


# pylint: disable=broad-except,no-else-return
def get_last_run_day_from_cloud_storage(
        bucket: str, object_key: str, number_of_previous_day_to_process=1
):
    """
    :param bucket:
    :param object_key:
    :param number_of_previous_day_to_process:
    :return:
    """
    try:
        date_as_string = download_s3_object(bucket, object_key)
        pattern = r"^\d\d\d\d-\d\d-\d\d$"
        if (
                date_as_string is not None
                and len(re.findall(pattern, date_as_string.strip())) == 1
        ):
            dtobj = datetime.datetime.strptime(
                date_as_string.strip(), ModuleConfig.STATE_FILE_DATE_FORMAT
            ) - timedelta(number_of_previous_day_to_process)
            return dtobj.strftime(ModuleConfig.STATE_FILE_DATE_FORMAT)
        else:
            return get_date_days_before_as_string(
                number_of_previous_day_to_process)
    except BaseException:
        return ModuleConfig.CROSSREF_DATA_COLLECTION_BEGINNING


# pylint: disable=fixme
def get_crossref_data_single_page(
        base_crossref_url: str,
        cursor=None,
        publisher_id: str = "",
        from_date_collected_as_string: str = "2019-08-01",
        message_key: str = "message",
):
    """
    :param base_crossref_url:
    :param cursor:
    :param publisher_id:
    :param from_date_collected_as_string:
    :param message_key:
    :return:
    """
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
    session = requests.Session()
    http_session_mount = requests.adapters.HTTPAdapter(max_retries=10)
    https_session_mount = requests.adapters.HTTPAdapter(max_retries=10)
    session.mount("http://", http_session_mount)
    session.mount("https://", https_session_mount)
    session_request = session.get(url)
    session_request.raise_for_status()
    resp = session_request.json()
    return resp[message_key][ModuleConfig.MESSAGE_NEXT_CURSOR_KEY], resp


# pylint: disable=broad-except,too-many-arguments
def write_result_to_file_get_latest_record_timestamp(
        json_list,
        full_temp_file_location: str,
        previous_latest_timestamp,
        imported_timestamp_key,
        imported_timestamp,
        schema
):
    """
    :param json_list:
    :param full_temp_file_location:
    :param previous_latest_timestamp:
    :param imported_timestamp_key:
    :param imported_timestamp:
    :param schema:
    :return:
    """
    latest_collected_record_timestamp = previous_latest_timestamp
    with open(full_temp_file_location, "a") as write_file:
        for record in json_list:
            n_record = cleanse_record_add_imported_timestamp_field(
                record, imported_timestamp_key,
                imported_timestamp, schema=schema)
            write_file.write(json.dumps(n_record))
            write_file.write("\n")
            try:
                record_collection_timestamp = datetime.datetime.strptime(
                    n_record.get(
                        ModuleConfig.CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY
                    ),
                    ModuleConfig.CROSSREF_TIMESTAMP_FORMAT
                )
            except Exception:
                record_collection_timestamp = latest_collected_record_timestamp
            latest_collected_record_timestamp = (
                latest_collected_record_timestamp
                if latest_collected_record_timestamp >
                record_collection_timestamp
                else record_collection_timestamp
            )
    return latest_collected_record_timestamp


def convert_bq_schema_field_list_to_dict(json_list, ):
    """
    :param json_list:
    :return:
    """
    k_name = ModuleConfig.BQ_SCHEMA_FIELD_NAME_KEY.lower()
    schema_list_as_dict = dict()
    for bq_schema_field in json_list:
        bq_field_name_list = [
            v for k, v in bq_schema_field.items() if k.lower() == k_name]
        bq_schema_field_lower_case_key_name = {
            k.lower(): v for k, v in bq_schema_field.items()}
        if len(bq_field_name_list) == 1:
            schema_list_as_dict[bq_field_name_list[0]
                                ] = bq_schema_field_lower_case_key_name
    return schema_list_as_dict


def standardize_field_name(field_name):
    """
    :param field_name:
    :return:
    """
    return re.sub(r'\W', '_', field_name)


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def semi_clean_crossref_record(record, schema):
    """
    :param record:
    :param schema:
    :return:
    """
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
                            ModuleConfig.BQ_SCHEMA_SUBFIELD_KEY
                        )
                    )
                if list_as_p_dict.get(new_key).get(
                        ModuleConfig.BQ_SCHEMA_FIELD_TYPE_KEY
                ).lower() == 'timestamp':
                    try:
                        datetime.datetime.strptime(
                            record_item_val,
                            ModuleConfig.CROSSREF_TIMESTAMP_FORMAT
                        )
                    except BaseException:
                        record_item_val = None
                new_dict[new_key] = record_item_val
        return new_dict
    elif isinstance(record, list):
        new_list = list()
        for elem in record:
            if isinstance(elem, (dict, list)):
                elem = semi_clean_crossref_record(elem, schema)
            if elem is not None:
                new_list.append(elem)
        return new_list


def cleanse_record_add_imported_timestamp_field(
        record, imported_timestamp_key, imported_timestamp, schema
):
    """
    :param record:
    :param imported_timestamp_key:
    :param imported_timestamp:
    :param schema:
    :return:
    """
    new_record = semi_clean_crossref_record(record, schema)
    new_record[imported_timestamp_key] = imported_timestamp
    return new_record


# pylint: disable=too-many-arguments
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
    """
    :param base_crossref_url:
    :param from_date_collected_as_string:
    :param publisher_id:
    :param message_key:
    :param event_key:
    :param imported_timestamp_key:
    :param imported_timestamp:
    :param full_temp_file_location:
    :param schema:
    :return:
    """
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
        "2000-01-01T00:00:00Z", ModuleConfig.CROSSREF_TIMESTAMP_FORMAT
    )
    latest_collected_record_timestamp = \
        write_result_to_file_get_latest_record_timestamp(
            results,
            full_temp_file_location,
            latest_collected_record_timestamp,
            imported_timestamp_key,
            imported_timestamp,
            schema=schema)

    while cursor:
        cursor, downloaded_data = get_crossref_data_single_page(
            base_crossref_url=base_crossref_url,
            cursor=cursor,
            publisher_id=publisher_id,
            from_date_collected_as_string=from_date_collected_as_string,
            message_key=message_key,
        )
        results = downloaded_data.get(message_key, {}).get(event_key, [])
        latest_collected_record_timestamp = \
            write_result_to_file_get_latest_record_timestamp(
                results,
                full_temp_file_location,
                latest_collected_record_timestamp,
                imported_timestamp_key,
                imported_timestamp,
                schema=schema
            )
    return latest_collected_record_timestamp


def add_timestamp_field_to_schema(schema_json, imported_timestamp_field_name):
    """
    :param schema_json:
    :param imported_timestamp_field_name:
    :return:
    """
    new_schema = [
        x for x in schema_json
        if imported_timestamp_field_name not in x.keys()]
    new_schema.append(
        {
            "mode": "NULLABLE",
            "name": imported_timestamp_field_name,
            "type": "TIMESTAMP",
        }
    )
    return new_schema


def current_timestamp_as_string():
    """
    :return:
    """
    dtobj = datetime.datetime.now(timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")


class CrossRefimportDataPipelineConfig:
    """
    parse config
    """
    # pylint: disable=too-many-instance-attributes, too-few-public-methods
    def __init__(self, data_config):
        self.project_name = data_config.get("PROJECT_NAME")
        self.dataset = data_config.get("DATASET")
        self.table = data_config.get("TABLE")
        self.imported_timestamp_field = data_config.get(
            "IMPORTED_TIMESTAMP_FIELD")
        self.schema_file_s3_bucket = data_config.get(
            "SCHEMA_FILE").get("BUCKET")
        self.state_file_name_key = data_config.get(
            "STATE_FILE").get("OBJECT_NAME")
        self.state_file_bucket = data_config.get("STATE_FILE").get("BUCKET")
        self.temp_file_dir = data_config.get("LOCAL_TEMPFILE_DIR")
        self.number_of_previous_day_to_process = data_config.get(
            "NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS"
        )
        self.message_key = data_config.get("MESSAGE_KEY")
        self.publisher_id = data_config.get("PUBLISHER_ID")
        self.crossref_event_base_url = data_config.get(
            "CROSSREF_EVENT_BASE_URL")
        self.event_key = data_config.get("EVENT_KEY")
        self.schema_file_object_name = data_config.get(
            "SCHEMA_FILE").get("OBJECT_NAME")
