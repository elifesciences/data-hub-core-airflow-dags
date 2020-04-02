import requests
import json
from json.decoder import JSONDecodeError
from requests.adapters import HTTPAdapter
from datetime import datetime
# pylint: disable=import-error
from requests.packages.urllib3.util.retry import Retry
import dateparser
from botocore.exceptions import ClientError

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string,
    download_s3_json_object,
    upload_s3_object
)
from typing import  Iterable
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    convert_bq_schema_field_list_to_dict,
    standardize_field_name
)

from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import write_result_to_file
from data_pipeline.utils.data_store.bq_data_service import load_file_into_bq


def requests_retry_session(
        retries=10,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_stored_state(
        data_config: WebApiConfig,
        initial_start_date: str
):
    try:
        state = download_s3_object_as_string(
            data_config.state_file_bucket_name,
            data_config.state_file_object_name
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            state = initial_start_date
        else:
            raise ex
    return state


# pylint: disable=fixme,too-many-arguments
def get_data_single_page(
        data_config: WebApiConfig,
        cursor: str = None,
        from_date: datetime = None,
        until_date: datetime = None,
        page_number: int = None,
) -> (str, dict):

    url = data_config.url_composer.get_url(
        from_date=from_date,
        to_date=until_date,
        cursor=cursor,
        page_number=page_number
    )
    with requests_retry_session() as session:
        session_request = session.get(url)
        session_request.raise_for_status()
        resp = session_request.content
        try:
            json_resp = json.loads(resp)
        except JSONDecodeError:
            json_resp = get_newline_delimited_json_string_as_json_list(
                resp.decode("utf-8")
            )
    return json_resp


def download_web_api_data(
        data_config: WebApiConfig,
        full_temp_file_location,
        from_date: datetime = None,
        until_date: datetime = None,
):
    cursor = None
    page_number = 1 if data_config.url_composer.page_number_param else None
    while True:
        page_data = get_data_single_page(
            data_config=data_config,
            from_date=from_date,
            until_date=until_date,
            cursor=cursor,
            page_number=page_number
        )
        cursor = get_next_cursor_from_data(page_data, data_config)
        page_number = get_next_page_number(page_data, page_number, data_config)

        items_list = get_items_list(
            page_data, data_config
        )
        bb = write_result_to_file(
            items_list,
            "/ll/tyuj"
        )
        sd = list(bb)
        if not cursor and not page_number:
            break

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=data_config.table_name,
        auto_detect_schema=False,
        dataset_name=data_config.dataset_name,
        #write_mode=write_disposition,
        project_name=data_config.gcp_project,
    )


def process_downloaded_data(
        record_list: list,
        data_config: WebApiConfig,
        data_etl_datetime,
        file_location,
        prev_page_latest_datetime=None
):

    provenance = {
        data_config.import_timestamp_field_name:
            data_etl_datetime
    }
    bq_schema = get_bq_schema(data_config)
    n_record_list = process_record_in_list(
        record_list=record_list, bq_schema=bq_schema,
        provenance=provenance
    )
    n_record_list = write_result_to_file(
        n_record_list, file_location
    )
    current_page_latest_datetime = get_latest_record_list_timestamp(
        n_record_list,prev_page_latest_datetime, data_config
    )

    return current_page_latest_datetime


def get_bq_schema(data_config: WebApiConfig,):
    if data_config.schema_file_object_name and data_config.schema_file_s3_bucket:
       bq_schema = download_s3_json_object(
           data_config.schema_file_s3_bucket,
           data_config.schema_file_object_name
       )
    else:
        bq_schema = None
    return bq_schema


def process_record_in_list(
        record_list,
        provenance: dict = None,
        bq_schema=None
) -> Iterable:
    for record in record_list:
        n_record = standardize_record_keys(record)
        if bq_schema:
            n_record = filter_record_by_schema(n_record, bq_schema)
        if provenance:
            n_record.update(provenance)
        yield n_record


def get_latest_record_list_timestamp(
        record_list, previous_latest_timestamp, data_config: WebApiConfig
):
    latest_collected_record_timestamp_list = [
        parse_datetime_from_str(
            get_dict_values_from_hierarchy_as_list(
                record,
                data_config.item_timestamp_key_hierarchy_from_item_root_as_list),
            data_config.item_timestamp_format,
        )
        for record in record_list
    ]
    latest_collected_record_timestamp_list.append(previous_latest_timestamp)
    latest_collected_record_timestamp_list = [
        timestamp for timestamp in latest_collected_record_timestamp_list if timestamp]
    return max(latest_collected_record_timestamp_list) if latest_collected_record_timestamp_list else None


def parse_datetime_from_str(datetime_as_str, time_format: str = None):
    if time_format:
        datetime_obj = datetime.strptime(datetime_as_str.strip(), time_format)
    else:
        datetime_obj = dateparser.parse(
            datetime_as_str.strip()
        )
    return datetime_obj


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def standardize_record_keys(record_object):
    if isinstance(record_object, dict):
        new_dict = {}
        for item_key, item_val in record_object.items():
            new_key = standardize_field_name(item_key)
            if isinstance(item_val, (list, dict)):
                item_val = standardize_record_keys(
                    item_val,
                )
                new_dict[new_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = list()
        for elem in record_object:
            if isinstance(elem, (dict, list)):
                elem = standardize_record_keys(
                    elem
                )
            if elem is not None:
                new_list.append(elem)
        return new_list


# pylint: disable=inconsistent-return-statements,broad-except,no-else-return
def filter_record_by_schema(record_object, record_object_schema):
    if isinstance(record_object, dict):
        list_as_p_dict = convert_bq_schema_field_list_to_dict(
            record_object_schema
        )
        key_list = set(list_as_p_dict.keys())
        new_dict = {}
        for item_key, item_val in record_object.items():
            if item_key in key_list:
                if isinstance(item_val, (list, dict)):
                    item_val = filter_record_by_schema(
                        item_val,
                        list_as_p_dict.get(item_key).get(
                            ModuleConstant.BQ_SCHEMA_SUBFIELD_KEY
                        ),
                    )
                if (
                        list_as_p_dict.get(item_key)
                        .get(ModuleConstant.BQ_SCHEMA_FIELD_TYPE_KEY)
                        .lower() == "timestamp"
                ):
                    try:
                        dateparser.parse(
                            item_val
                        )
                    except BaseException:
                        item_val = None
                new_dict[item_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = list()
        for elem in record_object:
            if isinstance(elem, (dict, list)):
                elem = filter_record_by_schema(
                    elem, record_object_schema
                )
            if elem is not None:
                new_list.append(elem)
        return new_list


def get_items_list(data, web_config):
    item_list = data
    if isinstance(data, dict):
        item_list = get_dict_values_from_hierarchy_as_list(
            data,
            web_config.items_key_hierarchy_from_response_root_as_list
        )
    return item_list


def get_next_page_number(data, current_page, web_config: WebApiConfig):
    item_list = []
    if web_config.url_composer.page_number_param:
        item_list = get_items_list(data, web_config)
    next_page = current_page + 1 if item_list else None
    return next_page


def get_next_cursor_from_data(data, web_config: WebApiConfig):
    next_cursor =None
    if web_config.url_composer.next_page_cursor:
        next_cursor = get_dict_values_from_hierarchy_as_list(
            data,
            web_config.next_page_cursor_key_hierarchy_from_response_root_as_list
        )
    return next_cursor


def get_newline_delimited_json_string_as_json_list(json_string):
    return [
        json.loads(line) for line in json_string.split("\n")
        if line.strip()
    ]


def get_dict_values_from_hierarchy_as_list(data, hierachy: list):
    data_value = data
    for list_element in hierachy:
        if data_value:
            data_value = data_value.get(
                list_element, None
            )
        else:
            break
    return data_value


# pylint: disable=too-few-public-methods
class ModuleConstant:

    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"
