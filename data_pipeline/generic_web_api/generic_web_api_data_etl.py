import dataclasses
import itertools
import os
import logging
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import json
from json.decoder import JSONDecodeError
from typing import Any, Iterable, Optional, Tuple, TypeVar, cast

import objsize

from botocore.exceptions import ClientError

from data_pipeline.generic_web_api.transform_data import (
    get_latest_record_list_timestamp,
    get_web_api_provenance,
    iter_processed_record_for_api_item_list_response,
    get_dict_values_from_path_as_list
)
from data_pipeline.generic_web_api.module_constants import ModuleConstant
from data_pipeline.utils.collections import iter_batch_iterable
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string,
    upload_s3_object
)
from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
    create_or_extend_table_schema
)
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.pipeline_file_io import iter_write_jsonl_to_file
from data_pipeline.utils.pipeline_utils import (
    get_response_and_provenance_from_api,
    iter_dict_for_bigquery_include_exclude_source_config
)
from data_pipeline.utils.progress import ProgressMonitor
from data_pipeline.utils.text import format_byte_count
from data_pipeline.utils.web_api import requests_retry_session_for_config

from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiConfig
)
from data_pipeline.generic_web_api.request_builder import (
    WebApiDynamicRequestParameters
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp,
    get_current_timestamp_as_string,
    datetime_to_string,
    parse_timestamp_from_str,
    get_tz_aware_datetime
)

LOGGER = logging.getLogger(__name__)


T = TypeVar('T')


def get_start_timestamp_from_state_file_or_optional_default_value(
    data_config: WebApiConfig
):
    try:
        stored_state = (
            parse_timestamp_from_str(
                download_s3_object_as_string(
                    data_config.state_file_bucket_name,
                    data_config.state_file_object_name
                )
            )
            if data_config.state_file_bucket_name and
            data_config.state_file_object_name else None
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = parse_timestamp_from_str(
                data_config.default_start_date
            ) if (
                data_config.default_start_date and
                data_config.dynamic_request_builder.date_format
            ) else None

        else:
            raise ex
    return stored_state


def get_newline_delimited_json_string_as_json_list(json_string):
    return [
        json.loads(line) for line in json_string.splitlines()
        if line.strip()
    ]


@dataclasses.dataclass(frozen=True)
class WebApiPageResponse:
    response_json: Any
    request_provenance: dict = dataclasses.field(default_factory=dict)


def get_data_single_page_response(
    data_config: WebApiConfig,
    dynamic_request_parameters: WebApiDynamicRequestParameters
) -> WebApiPageResponse:
    url = data_config.dynamic_request_builder.get_url(
        dynamic_request_parameters
    )
    json_data = data_config.dynamic_request_builder.get_json(
        dynamic_request_parameters=dynamic_request_parameters
    )
    LOGGER.info(
        "Request URL: %s %s (json: %r)",
        data_config.dynamic_request_builder.method,
        url,
        json_data
    )

    with requests_retry_session_for_config(
        data_config.dynamic_request_builder.retry_config
    ) as session:
        if (data_config.authentication and data_config.authentication.authentication_type):
            assert data_config.authentication.authentication_type == "basic"
            assert data_config.authentication.auth_val_list
            session.auth = cast(Tuple[str, str], tuple(data_config.authentication.auth_val_list))
        session.verify = False
        LOGGER.info("Headers: %s", data_config.headers)
        session_response, request_provenance = get_response_and_provenance_from_api(
            session=session,
            method=data_config.dynamic_request_builder.method,
            url=url,
            json_data=json_data,
            headers=data_config.headers.mapping,
            raise_on_status=True
        )
        LOGGER.info('Request Provenance: %r', request_provenance)
        resp = session_response.content
        try:
            json_resp = json.loads(resp)
        except JSONDecodeError:
            LOGGER.debug('Attempting to parse jsonl: %r', resp)
            json_resp = get_newline_delimited_json_string_as_json_list(
                resp.decode("utf-8")
            )
        parsed_response_size = objsize.get_deep_size(json_resp)
        LOGGER.info(
            'Response size: %s, parsed: %s',
            format_byte_count(len(resp)),
            format_byte_count(parsed_response_size)
        )
    return WebApiPageResponse(
        response_json=json_resp,
        request_provenance=request_provenance
    )


def get_data_single_page(*args, **kwargs) -> Any:
    page_response = get_data_single_page_response(*args, **kwargs)
    return page_response.response_json


def iter_optional_source_values_from_bigquery(
    data_config: WebApiConfig
) -> Optional[Iterable[dict]]:
    source_config = data_config.source
    if source_config is None:
        return None
    LOGGER.debug(
        'processing source config: %r',
        source_config
    )
    return iter_dict_for_bigquery_include_exclude_source_config(
        source_config
    )


def get_next_source_values_or_none(
    data_config: WebApiConfig,
    all_source_values_iterator: Optional[Iterable[dict]] = None
) -> Optional[Iterable[dict]]:
    if all_source_values_iterator is None:
        return None
    assert data_config.dynamic_request_builder.max_source_values_per_request
    return list(itertools.islice(
        all_source_values_iterator,
        data_config.dynamic_request_builder.max_source_values_per_request
    ))


def get_next_dynamic_request_parameters_for_page_data(  # pylint: disable=too-many-arguments
    page_data: Any,
    items_count: int,
    current_dynamic_request_parameters: WebApiDynamicRequestParameters,
    data_config: WebApiConfig,
    latest_record_timestamp: Optional[datetime] = None,
    fixed_until_date: Optional[datetime] = None,
    all_source_values_iterator: Optional[Iterable[dict]] = None
) -> Optional[WebApiDynamicRequestParameters]:
    if all_source_values_iterator is not None:
        # Note: added assert for currently unsupported other parameters when using source values
        assert not current_dynamic_request_parameters.cursor
        assert not current_dynamic_request_parameters.page_number
        assert not current_dynamic_request_parameters.page_offset
        assert not current_dynamic_request_parameters.from_date
        assert not current_dynamic_request_parameters.to_date
        next_source_values = get_next_source_values_or_none(
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )
        if not next_source_values:
            LOGGER.info('no more source values to process')
            return None
        return current_dynamic_request_parameters._replace(
            source_values=next_source_values
        )
    cursor = get_next_cursor_from_data(
        data=page_data,
        web_config=data_config,
        previous_cursor=current_dynamic_request_parameters.cursor
    )

    from_date_to_advance, to_reset_page_or_offset_param = (
        get_next_start_date(
            items_count=items_count,
            current_start_timestamp=current_dynamic_request_parameters.from_date,
            latest_record_timestamp=latest_record_timestamp,
            web_config=data_config,
            cursor=cursor,
            page_number=current_dynamic_request_parameters.page_number,
            offset=current_dynamic_request_parameters.page_offset
        )
    )
    page_number = get_next_page_number(
        items_count=items_count,
        current_page=current_dynamic_request_parameters.page_number,
        web_config=data_config,
        reset_param=to_reset_page_or_offset_param
    )
    offset = get_next_offset(
        items_count=items_count,
        current_offset=current_dynamic_request_parameters.page_offset,
        web_config=data_config,
        reset_param=to_reset_page_or_offset_param
    )

    variable_until_date = get_next_until_date(
        from_date=from_date_to_advance,
        data_config=data_config,
        fixed_until_date=fixed_until_date
    )

    if (
        cursor is None
        and page_number is None
        and from_date_to_advance is None
        and offset is None
    ):
        LOGGER.info('End reached, no next cursor, page number, offset or from date')
        return None
    return WebApiDynamicRequestParameters(
        from_date=from_date_to_advance or current_dynamic_request_parameters.from_date,
        to_date=variable_until_date,
        cursor=cursor,
        page_number=page_number,
        page_offset=offset,
        source_values=current_dynamic_request_parameters.source_values
    )


def get_initial_dynamic_request_parameters(
    data_config: WebApiConfig,
    initial_from_date: Optional[datetime] = None,
    fixed_until_date: Optional[datetime] = None,
    all_source_values_iterator: Optional[Iterable[dict]] = None
) -> Optional[WebApiDynamicRequestParameters]:
    initial_source_values = get_next_source_values_or_none(
        data_config=data_config,
        all_source_values_iterator=all_source_values_iterator
    )
    if all_source_values_iterator and not initial_source_values:
        LOGGER.info('no source values to process')
        return None
    return WebApiDynamicRequestParameters(
        from_date=initial_from_date,
        to_date=get_next_until_date(
            from_date=initial_from_date,
            data_config=data_config,
            fixed_until_date=fixed_until_date
        ),
        cursor=None,
        page_number=1 if data_config.dynamic_request_builder.page_number_param else None,
        page_offset=0 if data_config.dynamic_request_builder.offset_param else None,
        source_values=initial_source_values
    )


def iter_processed_web_api_data_etl_batch_data(
    data_config: WebApiConfig,
    initial_from_date: Optional[datetime] = None,
    until_date: Optional[datetime] = None,
    all_source_values_iterator: Optional[Iterable[dict]] = None
) -> Iterable[dict]:
    assert not isinstance(all_source_values_iterator, list)

    imported_timestamp = get_current_timestamp_as_string(
        ModuleConstant.DATA_IMPORT_TIMESTAMP_FORMAT
    )

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
    latest_record_timestamp: Optional[datetime] = None
    current_dynamic_request_parameters: Optional[WebApiDynamicRequestParameters] = (
        get_initial_dynamic_request_parameters(
            data_config=data_config,
            initial_from_date=initial_from_date,
            fixed_until_date=until_date,
            all_source_values_iterator=all_source_values_iterator
        )
    )
    if not current_dynamic_request_parameters:
        LOGGER.info('not data to process')
        return
    progress_monitor = ProgressMonitor(message_prefix='Processed records (before BigQuery): ')
    while current_dynamic_request_parameters:
        LOGGER.debug('current_dynamic_request_parameters=%r', current_dynamic_request_parameters)
        page_data = get_data_single_page(
            data_config=data_config,
            dynamic_request_parameters=current_dynamic_request_parameters
        )
        LOGGER.debug('page_data: %r', page_data)
        total_count = get_optional_total_count(page_data, data_config)
        if total_count:
            LOGGER.info('Total items (reported by API): %d', total_count)
            progress_monitor.set_total(total_count)
        items_list = get_items_list(
            page_data, data_config
        )
        LOGGER.debug('items_list: %r', items_list)
        if not items_list:
            LOGGER.info('Item list is empty, end reached')
            break
        items_list = remove_key_with_null_value(items_list)
        progress_monitor.increment(len(items_list))
        LOGGER.debug('items_list after removed null values: %r', items_list)
        processed_record_list = iter_processed_record_for_api_item_list_response(
            record_list=items_list,
            data_config=data_config,
            provenance=get_web_api_provenance(
                data_config=data_config,
                data_etl_timestamp=imported_timestamp
            )
        )

        for processed_record in processed_record_list:
            yield processed_record

            latest_record_timestamp = get_latest_record_list_timestamp(
                [processed_record],
                previous_latest_timestamp=latest_record_timestamp,
                data_config=data_config
            )

        LOGGER.info('%s', progress_monitor)

        LOGGER.debug('latest_record_timestamp: %r', latest_record_timestamp)
        items_count = len(items_list)
        current_dynamic_request_parameters = get_next_dynamic_request_parameters_for_page_data(
            page_data=page_data,
            items_count=items_count,
            latest_record_timestamp=latest_record_timestamp,
            fixed_until_date=until_date,
            current_dynamic_request_parameters=current_dynamic_request_parameters,
            data_config=data_config,
            all_source_values_iterator=all_source_values_iterator
        )

    if progress_monitor.is_incomplete():
        LOGGER.warning('Not all of the expected records received from API')


def iter_optional_batch_iterable(
    iterable: Iterable[T],
    batch_size: Optional[int]
) -> Iterable[Iterable[T]]:
    if not batch_size:
        LOGGER.debug('no batch_size configured')
        return [iterable]
    LOGGER.debug('batch_size: %r', batch_size)
    return iter_batch_iterable(iterable, batch_size)


def process_web_api_data_etl_batch(
    data_config: WebApiConfig,
    initial_from_date: Optional[datetime] = None,
    until_date: Optional[datetime] = None,
    all_source_values_iterator: Optional[Iterable[dict]] = None
):
    all_processed_record_iterable = iter_processed_web_api_data_etl_batch_data(
        data_config=data_config,
        initial_from_date=initial_from_date,
        until_date=until_date,
        all_source_values_iterator=all_source_values_iterator
    )
    LOGGER.debug('all_processed_record_list: %r', all_processed_record_iterable)
    for batch_index, batch_processed_record_iterable in enumerate(iter_optional_batch_iterable(
        all_processed_record_iterable,
        batch_size=data_config.batch_size
    )):
        LOGGER.debug('processed_record_list: %r', batch_processed_record_iterable)
        with TemporaryDirectory() as tmp_dir:
            full_temp_file_location = os.path.join(tmp_dir, 'downloaded_jsonl_data.jsonl')
            batch_processed_record_iterable = iter_write_jsonl_to_file(
                batch_processed_record_iterable,
                full_temp_file_location=full_temp_file_location
            )
            latest_record_timestamp = get_latest_record_list_timestamp(
                batch_processed_record_iterable,
                previous_latest_timestamp=None,
                data_config=data_config
            )
            LOGGER.debug('latest_record_timestamp (for state): %r', latest_record_timestamp)

            load_written_data_to_bq(
                data_config=data_config,
                full_temp_file_location=full_temp_file_location
            )
            if latest_record_timestamp:
                LOGGER.info('updating state to: %r', latest_record_timestamp)
                upload_latest_timestamp_as_pipeline_state(
                    data_config=data_config,
                    latest_record_timestamp=latest_record_timestamp
                )
            else:
                LOGGER.info('not updating state due to no latest record timestamp')
        LOGGER.info('completed batch: %d', 1 + batch_index)


def get_next_batch_from_timestamp_for_config(
    data_config: WebApiConfig,
    current_from_timestamp: Optional[datetime]
) -> Optional[datetime]:
    if data_config.start_to_end_date_diff_in_days and current_from_timestamp:
        return current_from_timestamp + timedelta(days=data_config.start_to_end_date_diff_in_days)
    return None


def generic_web_api_data_etl(
    data_config: WebApiConfig,
    end_timestamp: Optional[datetime] = None
):
    LOGGER.info('data_config: %r', data_config)
    stored_state = get_start_timestamp_from_state_file_or_optional_default_value(data_config)
    current_from_timestamp = stored_state
    if not end_timestamp and current_from_timestamp:
        end_timestamp = get_current_timestamp()
    LOGGER.info('end_timestamp: %r', end_timestamp)
    all_source_values_iterator = iter_optional_source_values_from_bigquery(data_config)
    while True:
        next_from_timestamp = get_next_batch_from_timestamp_for_config(
            data_config=data_config,
            current_from_timestamp=current_from_timestamp
        )
        process_web_api_data_etl_batch(
            data_config=data_config,
            initial_from_date=current_from_timestamp,
            until_date=next_from_timestamp,
            all_source_values_iterator=all_source_values_iterator
        )
        if (
            not next_from_timestamp
            or not end_timestamp
            or next_from_timestamp >= end_timestamp
        ):
            LOGGER.debug(
                'end reached, current_from_timestamp=%r, next_from_timestamp=%r',
                current_from_timestamp, next_from_timestamp
            )
            break
        current_from_timestamp = next_from_timestamp


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


def get_next_until_date(
    from_date: Optional[datetime],
    data_config: WebApiConfig,
    fixed_until_date: Optional[datetime]
):
    until_date = None
    if fixed_until_date:
        until_date = fixed_until_date
    elif (
        from_date
        and data_config.start_to_end_date_diff_in_days
    ):
        until_date = (
            from_date +
            timedelta(days=data_config.start_to_end_date_diff_in_days)
        )

    return until_date


def get_next_page_number(
    items_count, current_page,
    web_config: WebApiConfig,
    reset_param: bool = False
):
    next_page = None
    if web_config.dynamic_request_builder.page_number_param:
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
    if web_config.dynamic_request_builder.offset_param:
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


def get_next_start_date(  # pylint: disable=too-many-arguments
    items_count,
    current_start_timestamp,
    latest_record_timestamp,
    web_config: WebApiConfig,
    cursor: Optional[str] = None,
    page_number: Optional[int] = None,
    offset: Optional[int] = None
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
    elif (
        current_start_timestamp == latest_record_timestamp
        and not next_page_number and not next_offset
    ):
        from_timestamp = None
    elif (
        current_start_timestamp != latest_record_timestamp and
        (
            next_page_number or next_offset
            or not (
                web_config.dynamic_request_builder.offset_param
                or web_config.dynamic_request_builder.page_number_param
            )
        ) and
        web_config.response.item_timestamp_key_path_from_item_root and
        items_count
    ):

        from_timestamp = latest_record_timestamp
        reset_page_or_offset_param = True
    return from_timestamp, reset_page_or_offset_param


def get_next_cursor_from_data(
    data,
    web_config: WebApiConfig,
    previous_cursor: Optional[str]
) -> Optional[str]:
    LOGGER.debug('previous_cursor: %r', previous_cursor)
    if web_config.dynamic_request_builder.next_page_cursor:
        next_cursor = get_dict_values_from_path_as_list(
            data,
            web_config.response.next_page_cursor_key_path_from_response_root
        )
        if next_cursor and next_cursor == previous_cursor:
            if not web_config.dynamic_request_builder.allow_same_next_page_cursor:
                LOGGER.info('Ignoring cursor that is the same as previous cursor: %r', next_cursor)
                return None
            LOGGER.info(
                'Proceeding with cursor that is the same as previous cursor: %r',
                next_cursor
            )
        return next_cursor
    return None


def get_optional_total_count(page_data, web_config: WebApiConfig) -> Optional[int]:
    if web_config.response.total_item_count_key_path_from_response_root:
        return get_dict_values_from_path_as_list(
            page_data,
            web_config.response.total_item_count_key_path_from_response_root
        )
    return None


def get_items_list(page_data, web_config: WebApiConfig):
    item_list = page_data
    if isinstance(page_data, dict):
        item_list = get_dict_values_from_path_as_list(
            page_data,
            web_config.response.items_key_path_from_response_root
        )
    if item_list is None:
        LOGGER.error(
            'item list not found in response, key path: %r, page_data= %s',
            web_config.response.items_key_path_from_response_root,
            page_data
        )
        raise ValueError(
            f'item list not found in response, \
                key path: {web_config.response.items_key_path_from_response_root}'
        )
    if isinstance(item_list, dict):
        return [item_list]
    return item_list


def upload_latest_timestamp_as_pipeline_state(
    data_config: WebApiConfig,
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
