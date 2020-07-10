from datetime import datetime
from typing import Iterable, List

from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
)

from data_pipeline.utils.pipeline_file_io import iter_write_jsonl_to_file_process_batch

from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)
from data_pipeline.utils.data_pipeline_timestamp import (
    parse_timestamp_from_str
)
from data_pipeline.utils.record_processing import standardize_record_keys, filter_record_by_schema


def get_dict_values_from_path_as_list(
        page_data, path_keys: List[str]
):
    data_value = page_data
    for list_element in path_keys:
        data_value = extract_content_from_response(data_value, list_element)
        if not data_value:
            break
    return data_value


def extract_content_from_response(
        data_in_response, resp_path_level: str
):
    extracted_data = None
    if isinstance(data_in_response, dict) and resp_path_level:
        extracted_data = data_in_response.get(resp_path_level)
    elif isinstance(data_in_response, list) and resp_path_level:
        extracted_data = [
            elem.get(resp_path_level) for elem in data_in_response
        ]
    return extracted_data


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
    latest_collected_record_timestamp_list = [previous_latest_timestamp]

    for record in record_list:

        if data_config.item_timestamp_key_path_from_item_root:
            record_timestamp = parse_timestamp_from_str(
                get_dict_values_from_path_as_list(
                    record,
                    data_config.item_timestamp_key_path_from_item_root
                ),
                data_config.item_timestamp_format,
            )
            latest_collected_record_timestamp_list.append(record_timestamp)
    latest_collected_record_timestamp_list = [
        timestamp for
        timestamp in latest_collected_record_timestamp_list
        if timestamp
    ]
    latest_timestamp = max(
        latest_collected_record_timestamp_list
    ) if latest_collected_record_timestamp_list else None
    return latest_timestamp


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
    processed_record_list = process_record_in_list(
        record_list=record_list, bq_schema=bq_schema,
        provenance=provenance
    )
    processed_record_list = iter_write_jsonl_to_file_process_batch(
        processed_record_list, file_location
    )
    current_page_latest_timestamp = get_latest_record_list_timestamp(
        processed_record_list, prev_page_latest_timestamp, data_config
    )
    return current_page_latest_timestamp


def get_bq_schema(data_config: WebApiConfig,):
    if (
            data_config.schema_file_object_name and
            data_config.schema_file_s3_bucket
    ):
        bq_schema = download_s3_json_object(
            data_config.schema_file_s3_bucket,
            data_config.schema_file_object_name
        )
    else:
        bq_schema = None
    return bq_schema
