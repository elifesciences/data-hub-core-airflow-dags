from datetime import timezone, datetime
import json
from json.decoder import JSONDecodeError

from typing import Iterable
import dateparser
from botocore.exceptions import ClientError

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string,
    download_s3_json_object,
    upload_s3_object
)
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    create_table,
    extend_table_schema_with_nested_schema,
    load_file_into_bq
)
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    convert_bq_schema_field_list_to_dict,
    standardize_field_name,
    write_result_to_file,
    requests_retry_session
)

from data_pipeline.s3_csv_data.s3_csv_etl import generate_schema_from_file
from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig


def get_timestamp_as_string(timestamp: datetime):
    return timestamp.strftime(
        ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
    )


def get_current_timestamp_as_string():
    return datetime.now(
        timezone.utc
    ).strftime(ModuleConstant.DEFAULT_TIMESTAMP_FORMAT)


def parse_timestamp_from_str(timestamp_as_str, time_format: str = None):
    if time_format:
        timestamp_obj = datetime.strptime(timestamp_as_str.strip(), time_format)
    else:
        timestamp_obj = dateparser.parse(
            timestamp_as_str.strip()
        )
    return timestamp_obj


def get_stored_state(
        data_config: WebApiConfig,
        initial_start_date: str = None
):
    try:
        state = (
            download_s3_object_as_string(
                data_config.state_file_bucket_name,
                data_config.state_file_object_name
            ) if data_config.state_file_bucket_name
            and data_config.state_file_object_name else None
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            state = initial_start_date
        else:
            raise ex
    return state


def get_newline_delimited_json_string_as_json_list(json_string):
    return [
        json.loads(line) for line in json_string.split("\n")
        if line.strip()
    ]


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
    imported_timestamp = get_current_timestamp_as_string()
    stored_state = get_stored_state(
        data_config
    )
    latest_record_timestamp = parse_timestamp_from_str(
        stored_state, ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
    ) if stored_state else None
    page_number = 1 if data_config.url_composer.page_number_param else None
    while True:
        page_data = get_data_single_page(
            data_config=data_config,
            from_date=from_date,
            until_date=until_date,
            cursor=cursor,
            page_number=page_number,
        )
        cursor = get_next_cursor_from_data(page_data, data_config)
        page_number = get_next_page_number(
            page_data, page_number, data_config
        )

        items_list = get_items_list(
            page_data, data_config
        )
        latest_record_timestamp = process_downloaded_data(
            data_config=data_config,
            record_list=items_list,
            data_etl_timestamp=imported_timestamp,
            file_location=full_temp_file_location,
            prev_page_latest_timestamp=latest_record_timestamp
        )
        if not cursor and not page_number:
            break

    create_n_extend_table(data_config, full_temp_file_location)

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=data_config.table_name,
        auto_detect_schema=False,
        dataset_name=data_config.dataset_name,
        project_name=data_config.gcp_project,
    )
    upload_latest_timestamp(
        data_config, latest_record_timestamp
    )


def get_next_page_number(data, current_page, web_config: WebApiConfig):
    item_list = []
    if web_config.url_composer.page_number_param:
        item_list = get_items_list(data, web_config)
    next_page = current_page + 1 if item_list else None
    return next_page


def get_next_cursor_from_data(data, web_config: WebApiConfig):
    next_cursor = None
    if web_config.url_composer.next_page_cursor:
        next_cursor = get_dict_values_from_hierarchy_as_list(
            data,
            web_config.next_page_cursor_key_hierarchy_from_response_root_as_list
        )
    return next_cursor


def get_items_list(data, web_config):
    item_list = data
    if isinstance(data, dict):
        item_list = get_dict_values_from_hierarchy_as_list(
            data,
            web_config.items_key_hierarchy_from_response_root_as_list
        )
    return item_list


def upload_latest_timestamp(data_config, latest_record_timestamp: datetime):
    latest_record_date = get_timestamp_as_string(latest_record_timestamp)
    state_file_name_key = data_config.state_file_object_name
    state_file_bucket = data_config.state_file_bucket_name
    upload_s3_object(
        bucket=state_file_bucket,
        object_key=state_file_name_key,
        data_object=latest_record_date,
    )


def create_n_extend_table(
        data_config: WebApiConfig,
        full_temp_file_location,
):
    schema = generate_schema_from_file(
        full_temp_file_location
    )
    if does_bigquery_table_exist(
            data_config.gcp_project,
            data_config.dataset_name,
            data_config.table_name,
    ):
        extend_table_schema_with_nested_schema(
            data_config.gcp_project,
            data_config.dataset_name,
            data_config.table_name,
            schema
        )
    else:
        create_table(
            data_config.gcp_project,
            data_config.dataset_name,
            data_config.table_name,
            schema
        )


def get_bq_schema(data_config: WebApiConfig,):
    if data_config.schema_file_object_name and data_config.schema_file_s3_bucket:
       bq_schema = download_s3_json_object(
           data_config.schema_file_s3_bucket,
           data_config.schema_file_object_name
       )
    else:
        bq_schema = None
    return bq_schema


def process_downloaded_data(
        record_list: list,
        data_config: WebApiConfig,
        data_etl_timestamp,
        file_location,
        prev_page_latest_timestamp: datetime = None
):
    provenance = {
        data_config.import_timestamp_field_name:
            data_etl_timestamp
    }
    bq_schema = get_bq_schema(data_config)
    n_record_list = process_record_in_list(
        record_list=record_list, bq_schema=bq_schema,
        provenance=provenance
    )
    n_record_list = write_result_to_file(
        n_record_list, file_location
    )
    current_page_latest_timestamp = get_latest_record_list_timestamp(
        n_record_list, prev_page_latest_timestamp, data_config
    )
    return current_page_latest_timestamp


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


def get_latest_record_list_timestamp(
        record_list, previous_latest_timestamp, data_config: WebApiConfig
):
    latest_timestamp = None
    if data_config.item_timestamp_key_hierarchy_from_item_root_as_list:
        latest_collected_record_timestamp_list = [
            parse_timestamp_from_str(
                get_dict_values_from_hierarchy_as_list(
                    record,
                    data_config.item_timestamp_key_hierarchy_from_item_root_as_list),
                data_config.item_timestamp_format,
            )
            for record in record_list
        ]
        latest_collected_record_timestamp_list.append(previous_latest_timestamp)
        latest_collected_record_timestamp_list = [
            timestamp for
            timestamp in latest_collected_record_timestamp_list
            if timestamp
        ]
        latest_timestamp = max(
            latest_collected_record_timestamp_list
        ) if latest_collected_record_timestamp_list else None
    return latest_timestamp


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
    DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"
