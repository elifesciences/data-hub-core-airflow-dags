from contextlib import contextmanager
import os
import io
import logging
from tempfile import TemporaryDirectory
from pathlib import Path
import csv
from csv import DictReader
import json
from typing import Iterable, Iterator, Mapping, Optional

from data_pipeline.s3_csv_data.s3_csv_state import CsvState, convert_datetime_string_to_datetime
from data_pipeline.utils.data_pipeline_timestamp import get_current_timestamp_as_string
from data_pipeline.s3_csv_data.s3_csv_config import (
    S3BaseCsvConfig,
    get_default_initial_s3_last_modified_date
)
from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
    create_or_extend_table_schema
)
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    standardize_field_name,
    get_write_disposition
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    get_s3_object_etag,
    iter_sorted_new_s3_files_to_process,
    s3_open_binary_read_with_temp_file,
    upload_s3_object
)
from data_pipeline.utils.record_processing import (
    process_record_values, DEFAULT_PROCESSING_STEPS
)
from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file

LOGGER = logging.getLogger(__name__)


def upload_s3_object_json(
    state_dict: Mapping[str, str],
    statefile_s3_bucket: str,
    statefile_s3_object: str
):
    upload_s3_object(
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object,
        data_object=json.dumps(state_dict)
    )


def get_stored_state(
    data_config: S3BaseCsvConfig,
    default_latest_file_date: str
) -> CsvState:
    try:
        return CsvState.from_dict(json.loads(
            download_s3_object_as_string_or_file_not_found_error(
                data_config.state_file_bucket_name,
                data_config.state_file_object_name
            )
        ))
    except FileNotFoundError:
        return CsvState.get_initial_state(
            object_patterns=data_config.s3_object_key_pattern_list,
            last_modified_datetime=convert_datetime_string_to_datetime(
                default_latest_file_date
            )
        )


def get_standardized_csv_header(
        record_list: list,
        csv_config: S3BaseCsvConfig
):
    csv_header = record_list[csv_config.header_line_index].split(",")
    standardized_csv_header = [
        standardize_field_name(field.lower()) for field in csv_header
        if field.strip() != ""
    ]
    return standardized_csv_header


def get_csv_dict_reader(
        csv_string: str,
        standardized_csv_header: list,
        csv_config: S3BaseCsvConfig
):
    csv_string_stream = io.StringIO(
        csv_string
    )
    skip_stream_till_line(
        csv_string_stream,
        csv_config.data_values_start_line_index
    )
    return csv.DictReader(
        csv_string_stream,
        fieldnames=standardized_csv_header
    )


def get_record_metadata(
        record_list: list,
        csv_config: S3BaseCsvConfig,
        s3_object_name: str,
        record_import_timestamp_as_string: str
):
    record_metadata = {
        metadata_col_name: record_list[line_index_in_data]
        for metadata_col_name, line_index_in_data
        in csv_config.in_sheet_record_metadata.items()
    }

    record_metadata[
        csv_config.import_timestamp_field_name
    ] = record_import_timestamp_as_string

    record_metadata.update(csv_config.fixed_sheet_record_metadata)
    record_metadata = update_metadata_with_provenance(
        record_metadata, csv_config.s3_bucket_name, s3_object_name
    )
    return record_metadata


def update_metadata_with_provenance(
        record_metadata, s3_bucket, s3_object
):
    provenance = {
        NamedLiterals.PROVENANCE_S3_BUCKET_FIELD_NAME:
            s3_bucket,
        NamedLiterals.PROVENANCE_S3_OBJECT_FIELD_NAME:
            s3_object
    }
    return {
        **record_metadata,
        NamedLiterals.PROVENANCE_FIELD_NAME:
            provenance
    }


@contextmanager
def iter_transformed_json_from_csv(
    s3_object_name: str,
    csv_config: S3BaseCsvConfig,
    record_import_timestamp_as_string: str,
) -> Iterator[Iterable[dict]]:
    default_value_processing_function_steps = (
        [*DEFAULT_PROCESSING_STEPS]
    )
    LOGGER.info('processing object: "%s"', s3_object_name)

    with s3_open_binary_read_with_temp_file(
        csv_config.s3_bucket_name,
        s3_object_name
    ) as streaming_body:
        LOGGER.debug('streaming_body: %s', streaming_body)
        text_stream = io.TextIOWrapper(streaming_body, 'utf-8')
        header_lines = [
            text_stream.readline() for _ in range(csv_config.data_values_start_line_index or 1)
        ]
        LOGGER.debug('header_lines: %s', header_lines)
        record_metadata = get_record_metadata(
            header_lines,
            csv_config,
            s3_object_name,
            record_import_timestamp_as_string
        )

        standardized_csv_header = get_standardized_csv_header(
            header_lines,
            csv_config
        )
        LOGGER.debug('standardized_csv_header: %s', standardized_csv_header)

        csv_dict_reader = csv.DictReader(
            text_stream,
            fieldnames=standardized_csv_header
        )
        if csv_config.record_processing_function_steps:
            default_value_processing_function_steps.extend(
                csv_config.record_processing_function_steps
            )
        processed_record_iterable = process_record_list(
            csv_dict_reader,
            record_metadata,
            default_value_processing_function_steps
        )

        yield processed_record_iterable


def transform_load_data(
    s3_object_name: str,
    csv_config: S3BaseCsvConfig,
    record_import_timestamp_as_string: str,
):
    with iter_transformed_json_from_csv(
        s3_object_name,
        csv_config,
        record_import_timestamp_as_string
    ) as processed_record_iterable:
        with TemporaryDirectory() as tmp_dir:
            full_temp_file_location = str(
                Path(tmp_dir, "downloaded_jsonl_data")
            )
            write_jsonl_to_file(
                processed_record_iterable,
                full_temp_file_location
            )

            if os.path.getsize(full_temp_file_location) > 0:
                create_or_extend_table_schema(
                    csv_config.gcp_project,
                    csv_config.dataset_name,
                    csv_config.table_name,
                    full_temp_file_location,
                    quoted_values_are_strings=False
                )
                write_disposition = get_write_disposition(csv_config)

                load_file_into_bq(
                    filename=full_temp_file_location,
                    table_name=csv_config.table_name,
                    auto_detect_schema=False,
                    dataset_name=csv_config.dataset_name,
                    write_mode=write_disposition,
                    project_name=csv_config.gcp_project,
                )


def skip_stream_till_line(text_stream, till_line_index):
    for _ in range(till_line_index):
        text_stream.readline()


def process_record_list(
        reader: DictReader,
        record_metadata: dict,
        value_processing_function_steps: Optional[list] = None
):
    for record in reader:
        n_record = merge_record_with_metadata(
            record=record,
            record_metadata=record_metadata,
        )
        n_record.pop(None, None)
        if value_processing_function_steps:
            n_record = process_record_values(
                n_record, value_processing_function_steps
            )
        yield n_record


def merge_record_with_metadata(
        record: dict,
        record_metadata: dict
):
    return {
        **record,
        **record_metadata
    }


class NamedLiterals:
    DAG_RUN = 'dag_run'
    RUN_ID = 'run_id'
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    S3_FILE_METADATA_LAST_MODIFIED_KEY = "LastModified"
    DEFAULT_AWS_CONN_ID = "aws_default"
    PROVENANCE_FIELD_NAME = "provenance"
    PROVENANCE_S3_BUCKET_FIELD_NAME = "s3_bucket"
    PROVENANCE_S3_OBJECT_FIELD_NAME = "source_filename"


def etl_new_csv_files(data_config: S3BaseCsvConfig):
    csv_state = get_stored_state(
        data_config,
        get_default_initial_s3_last_modified_date()
    )
    LOGGER.info('csv_state: %r', csv_state)
    new_s3_files = list(iter_sorted_new_s3_files_to_process(
        obj_pattern_with_latest_dates={
            object_pattern: object_pattern_csv_state.last_modified_datetime
            for object_pattern, object_pattern_csv_state in csv_state.state_dict.items()
        },
        s3_bucket_name=data_config.s3_bucket_name
    ))
    if not new_s3_files:
        LOGGER.info('No new file found and skipped the task.')
        return
    for matching_file_metadata_with_object_pattern in new_s3_files:
        matching_file_metadata = matching_file_metadata_with_object_pattern.file_metadata
        etag = get_s3_object_etag(
            bucket=matching_file_metadata.bucket,
            object_key=matching_file_metadata.name
        )
        LOGGER.info(
            'ETag for s3://%s/%s is %r',
            matching_file_metadata.bucket,
            matching_file_metadata.name,
            etag
        )
        record_import_timestamp_as_string = get_current_timestamp_as_string()
        object_key_pattern = matching_file_metadata_with_object_pattern.object_key_pattern
        transform_load_data(
            matching_file_metadata.name,
            data_config,
            record_import_timestamp_as_string,
        )
        csv_state.update_last_modified_datetime(
            object_pattern=object_key_pattern,
            last_modified_datetime=matching_file_metadata.last_modified
        )
        upload_s3_object_json(
            state_dict=csv_state.to_dict(),
            statefile_s3_bucket=data_config.state_file_bucket_name,
            statefile_s3_object=data_config.state_file_object_name
        )
