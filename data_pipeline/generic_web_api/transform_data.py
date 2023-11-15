import logging
from datetime import datetime
from typing import Iterable, List, Optional, Sequence
import dateparser

from data_pipeline.generic_web_api.module_constants import ModuleConstant
from data_pipeline.utils.collections import consume_iterable
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
        record_list: Iterable[dict],
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


def _is_valid_timestamp_string(timestamp_str: str) -> bool:
    try:
        if dateparser.parse(timestamp_str):
            return True
    except BaseException:
        pass
    return False


def _get_valid_timestamp_string_or_none(
    timestamp_str: str,
    key: str
) -> Optional[str]:
    if timestamp_str is None:
        return None
    if _is_valid_timestamp_string(timestamp_str):
        return timestamp_str
    LOGGER.warning('ignoring invalid timestamp value (key: %r): %r', key, timestamp_str)
    return None


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
                    item_val = _get_valid_timestamp_string_or_none(item_val, key=item_key)
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


def iter_record_timestamp_from_record_list(
    record_list: Iterable[dict],
    item_timestamp_key_path_from_item_root: Sequence[str]
) -> Iterable[datetime]:
    if not item_timestamp_key_path_from_item_root:
        return
    for record in record_list:
        record_timestamp_str_or_list = get_dict_values_from_path_as_list(
            record,
            item_timestamp_key_path_from_item_root
        )
        if record_timestamp_str_or_list is None:
            raise KeyError(
                f'record timestamp not found, path={repr(item_timestamp_key_path_from_item_root)}'
                f', record={repr(record)}'
            )
        LOGGER.debug('record_timestamp_str_or_list: %r', record_timestamp_str_or_list)
        record_timestamp_str_list = (
            record_timestamp_str_or_list if isinstance(record_timestamp_str_or_list, list)
            else [record_timestamp_str_or_list]
        )
        for timestamp_str in record_timestamp_str_list:
            timestamp = parse_timestamp_from_str(timestamp_str)
            if timestamp:
                yield timestamp


def get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
    record_list: Iterable[dict],
    previous_latest_timestamp: Optional[datetime],
    item_timestamp_key_path_from_item_root: Optional[Sequence[str]]
) -> Optional[datetime]:
    latest_collected_record_timestamp_list: List[datetime] = []
    if previous_latest_timestamp:
        latest_collected_record_timestamp_list.append(previous_latest_timestamp)
    if item_timestamp_key_path_from_item_root:
        latest_collected_record_timestamp_list.extend(
            iter_record_timestamp_from_record_list(
                record_list,
                item_timestamp_key_path_from_item_root
            )
        )
    else:
        consume_iterable(record_list)
    latest_timestamp = max(
        latest_collected_record_timestamp_list
    ) if latest_collected_record_timestamp_list else None
    return latest_timestamp


def get_latest_record_list_timestamp(
    record_list: Iterable[dict],
    previous_latest_timestamp: Optional[datetime],
    data_config: WebApiConfig
):
    return get_latest_record_list_timestamp_for_item_timestamp_key_path_from_item_root(
        record_list,
        previous_latest_timestamp=previous_latest_timestamp,
        item_timestamp_key_path_from_item_root=data_config.item_timestamp_key_path_from_item_root
    )


def iter_processed_record_for_api_item_list_response(
    record_list: Iterable[dict],
    data_config: WebApiConfig,
    provenance: dict
) -> Iterable[dict]:
    bq_schema = get_bq_schema(data_config)
    return process_record_in_list(
        record_list=record_list,
        bq_schema=bq_schema,
        provenance=provenance
    )


def get_web_api_provenance(
    data_config: WebApiConfig,
    data_etl_timestamp: datetime
) -> dict:
    return {
        data_config.import_timestamp_field_name: data_etl_timestamp
    }


def process_downloaded_data(
        record_list: Iterable[dict],
        data_config: WebApiConfig,
        data_etl_timestamp,
        file_location,
        prev_page_latest_timestamp: Optional[datetime] = None
):
    processed_record_list = iter_processed_record_for_api_item_list_response(
        record_list=record_list,
        data_config=data_config,
        provenance=get_web_api_provenance(
            data_config=data_config,
            data_etl_timestamp=data_etl_timestamp
        )
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
