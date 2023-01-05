import logging
from datetime import datetime
from typing import Iterable, Optional, Sequence
import dateparser

from data_pipeline.generic_web_api.module_constants import ModuleConstant
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
)

from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    convert_bq_schema_field_list_to_dict,
    standardize_field_name,
)
from data_pipeline.utils.pipeline_file_io import iter_write_jsonl_to_file

from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)
from data_pipeline.utils.data_pipeline_timestamp import (
    parse_timestamp_from_str
)


LOGGER = logging.getLogger(__name__)


def get_dict_values_from_path_as_list(
        page_data, path_keys: Sequence[str]
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
        provenance: Optional[dict] = None,
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
            if item_val:
                new_dict[new_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = []
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
    if isinstance(record_object, dict):  # pylint: disable=too-many-nested-blocks
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
                        if item_val is not None and not dateparser.parse(item_val):
                            LOGGER.warning('ignoring invalid timestamp value: %r', item_val)
                            item_val = None
                    except BaseException:
                        item_val = None
                new_dict[item_key] = item_val
        return new_dict
    elif isinstance(record_object, list):
        new_list = []
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
    latest_collected_record_timestamp_list = [previous_latest_timestamp]

    for record in record_list:

        if data_config.item_timestamp_key_path_from_item_root:
            record_timestamp = parse_timestamp_from_str(
                get_dict_values_from_path_as_list(
                    record,
                    data_config.item_timestamp_key_path_from_item_root
                )
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
        prev_page_latest_timestamp: Optional[datetime] = None
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
    processed_record_list = iter_write_jsonl_to_file(
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
