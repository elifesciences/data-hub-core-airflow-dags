import os
import logging
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
from json.decoder import JSONDecodeError

from botocore.exceptions import ClientError

from data_pipeline.generic_web_api.transform_data import (
    process_downloaded_data,
    get_dict_values_from_path_as_list
)
from data_pipeline.generic_web_api.module_constants import ModuleConstant
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string,
    upload_s3_object
)
from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
    create_or_extend_table_schema

)
from data_pipeline.utils.web_api import requests_retry_session

from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)
from data_pipeline.generic_web_api.url_builder import (
    UrlComposeParam
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string,
    datetime_to_string,
    parse_timestamp_from_str,
    get_tz_aware_datetime
)

LOGGER = logging.getLogger(__name__)


def get_stored_state(
        data_config: WebApiConfig,
):
    try:
        stored_state = (
            parse_timestamp_from_str(
                download_s3_object_as_string(
                    data_config.state_file_bucket_name,
                    data_config.state_file_object_name
                ),
                ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
            )
            if data_config.state_file_bucket_name and
            data_config.state_file_object_name else None
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = parse_timestamp_from_str(
                data_config.default_start_date,
                data_config.url_builder.date_format
            ) if (
                data_config.default_start_date and
                data_config.url_builder.date_format
            ) else None

        else:
            raise ex
    return stored_state


def get_newline_delimited_json_string_as_json_list(json_string):
    return [
        json.loads(line) for line in json_string.splitlines()
        if line.strip()
    ]


# pylint: disable=fixme,too-many-arguments
def get_data_single_page(
        data_config: WebApiConfig,
        cursor: str = None,
        from_date: datetime = None,
        until_date: datetime = None,
        page_number: int = None,
        page_offset: int = None
) -> (str, dict):
    url_compose_arg = UrlComposeParam(
        from_date=from_date,
        to_date=until_date,
        page_offset=page_offset,
        cursor=cursor,
        page_number=page_number
    )
    url = data_config.url_builder.get_url(
        url_compose_arg
    )
    LOGGER.info("Request URL: %s", url)

    with requests_retry_session() as session:
        if (
                data_config.authentication and
                data_config.authentication.authentication_type == "basic"
        ):
            session.auth = tuple(data_config.authentication.auth_val_list)
        session.verify = False
        session_response = session.get(url)
        session_response.raise_for_status()
        resp = session_response.content
        try:
            json_resp = json.loads(resp)
        except JSONDecodeError:
            json_resp = get_newline_delimited_json_string_as_json_list(
                resp.decode("utf-8")
            )
    return json_resp


# pylint: disable=too-many-locals
def generic_web_api_data_etl(
        data_config: WebApiConfig,
        from_date: datetime = None,
        until_date: datetime = None,
):
    imported_timestamp = get_current_timestamp_as_string(
        ModuleConstant.DATA_IMPORT_TIMESTAMP_FORMAT
    )

    stored_state = get_stored_state(
        data_config
    )
    initial_from_date = from_date or stored_state
    from_date_to_advance = initial_from_date
    LOGGER.info(
        "Running ETL from date %s, to date %s",
        datetime_to_string(
            initial_from_date,
            ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
        ),
        datetime_to_string(
            until_date,
            ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
        )
    )
    cursor = None
    latest_record_timestamp = None
    variable_until_date = get_next_until_date(
        from_date_to_advance, data_config, until_date
    )
    offset = 0 if data_config.url_builder.offset_param else None
    page_number = 1 if data_config.url_builder.page_number_param else None
    with TemporaryDirectory() as tmp_dir:
        full_temp_file_location = str(
            Path(tmp_dir, "downloaded_jsonl_data")
        )
        while True:

            page_data = get_data_single_page(
                data_config=data_config,
                from_date=from_date_to_advance or initial_from_date,
                until_date=variable_until_date,
                cursor=cursor,
                page_number=page_number,
                page_offset=offset
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
            items_count = len(items_list)
            cursor = get_next_cursor_from_data(page_data, data_config)

            from_date_to_advance, to_reset_page_or_offset_param = (
                get_next_start_date(
                    items_count, from_date_to_advance,
                    latest_record_timestamp, data_config,
                    cursor, page_number, offset
                )
            )
            page_number = get_next_page_number(
                items_count, page_number,
                data_config, to_reset_page_or_offset_param
            )
            offset = get_next_offset(
                items_count, offset, data_config,
                to_reset_page_or_offset_param
            )

            variable_until_date = get_next_until_date(
                from_date_to_advance, data_config, until_date
            )

            if (
                    cursor is None and page_number is None and
                    from_date_to_advance is None and offset is None
            ):
                break

        load_written_data_to_bq(data_config, full_temp_file_location)
        upload_latest_timestamp_as_pipeline_state(
            data_config, latest_record_timestamp
        )


def load_written_data_to_bq(
        data_config: WebApiConfig,
        full_temp_file_location: str
):
    if os.path.getsize(full_temp_file_location) > 0:
        create_or_extend_table_schema(
            data_config.gcp_project,
            data_config.dataset_name,
            data_config.table_name,
            full_temp_file_location,
            quoted_values_are_strings=False
        )

        load_file_into_bq(
            filename=full_temp_file_location,
            table_name=data_config.table_name,
            auto_detect_schema=False,
            dataset_name=data_config.dataset_name,
            project_name=data_config.gcp_project,
            write_mode=data_config.table_write_disposition
        )


def get_next_until_date(from_date: datetime, data_config, fixed_until_date):
    until_date = None
    if fixed_until_date:
        until_date = fixed_until_date
    elif (
            from_date
            and data_config.start_till_end_date_diff_in_days
    ):
        until_date = (
            from_date +
            timedelta(days=data_config.start_till_end_date_diff_in_days)
        )

    return until_date


def get_next_page_number(
        items_count, current_page,
        web_config: WebApiConfig,
        reset_param: bool = False
):
    next_page = None
    if web_config.url_builder.page_number_param:
        if reset_param:
            next_page = 1
        else:
            has_more_items = (
                items_count == web_config.page_size
                if web_config.page_size
                else items_count
            )
            next_page = current_page + 1 if has_more_items else None
    return next_page


def get_next_offset(
        items_count, current_offset,
        web_config: WebApiConfig,
        reset_param: bool = False
):

    next_offset = None
    if web_config.url_builder.offset_param:
        if reset_param:
            next_offset = 0
        else:
            has_more_items = (
                items_count == web_config.page_size
                if web_config.page_size
                else items_count
            )
            next_offset = (
                current_offset + web_config.page_size
                if has_more_items else None
            )
    return next_offset


def get_next_start_date(
        items_count,
        current_start_timestamp,
        latest_record_timestamp,
        web_config: WebApiConfig,
        cursor: str = None,
        page_number: int = None,
        offset: int = None

):
    # pylint: disable=too-many-boolean-expressions
    from_timestamp = None
    reset_page_or_offset_param = False
    next_page_number = get_next_page_number(
        items_count, page_number, web_config, False
    )
    next_offset = get_next_offset(
        items_count, offset, web_config, False
    )
    if cursor or next_page_number or next_offset:
        from_timestamp = current_start_timestamp
    elif(
            current_start_timestamp == latest_record_timestamp
            and not next_page_number and not next_offset
    ):
        from_timestamp = None
    elif (
            current_start_timestamp != latest_record_timestamp and
            (
                next_page_number or next_offset
                or not (
                    web_config.url_builder.offset_param
                    or web_config.url_builder.page_number_param
                )
            ) and
            web_config.item_timestamp_key_path_from_item_root and
            items_count
    ):

        from_timestamp = latest_record_timestamp
        reset_page_or_offset_param = True
    return from_timestamp, reset_page_or_offset_param


def get_next_cursor_from_data(data, web_config: WebApiConfig):
    next_cursor = None

    if web_config.url_builder.next_page_cursor:
        next_cursor = get_dict_values_from_path_as_list(
            data,
            web_config.next_page_cursor_key_path_from_response_root
        )
    return next_cursor


def get_items_list(page_data, web_config):
    item_list = page_data
    if isinstance(page_data, dict):
        item_list = get_dict_values_from_path_as_list(
            page_data,
            web_config.items_key_path_from_response_root
        )
    if item_list is None:
        LOGGER.error(
            'item list not found in response, key path: %r, page_data=%s',
            web_config.items_key_path_from_response_root,
            page_data
        )
        raise ValueError(
            'item list not found in response, key path: %r'
            % web_config.items_key_path_from_response_root
        )
    return item_list


def upload_latest_timestamp_as_pipeline_state(
        data_config,
        latest_record_timestamp: datetime
):
    if (
            data_config.state_file_object_name and
            data_config.state_file_bucket_name
    ):
        latest_record_date = datetime_to_string(
            get_tz_aware_datetime(latest_record_timestamp),
            ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
        )
        state_file_name_key = data_config.state_file_object_name
        state_file_bucket = data_config.state_file_bucket_name
        upload_s3_object(
            bucket=state_file_bucket,
            object_key=state_file_name_key,
            data_object=latest_record_date,
        )
