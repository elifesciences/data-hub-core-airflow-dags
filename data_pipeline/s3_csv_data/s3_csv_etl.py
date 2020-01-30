import io
import logging
import csv
from csv import DictReader
import json
from datetime import timezone, datetime

from botocore.exceptions import ClientError
from dateutil import tz

from data_pipeline.s3_csv_data.s3_csv_config import S3CsvConfig
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    load_file_into_bq,
    extend_table_schema_field_names,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    get_new_table_columns_schema,
    standardize_field_name,
    write_to_file,
    get_write_disposition
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
    upload_s3_object
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string
)

LOGGER = logging.getLogger(__name__)
DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE = "2019-10-10 21:10:13"


def convert_datetime_string_to_datetime(
        datetime_as_string: str,
        time_format: str = "%Y-%m-%d %H:%M:%S"
) -> datetime:
    tz_unaware = datetime.strptime(datetime_as_string.strip(), time_format)
    tz_aware = tz_unaware.replace(tzinfo=tz.tzlocal())

    return tz_aware


def convert_datetime_to_string(dtobj, dt_format="%Y-%m-%d %H:%M:%S"):
    return dtobj.strftime(dt_format)


def current_timestamp_as_string():
    dtobj = datetime.now(timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")


def update_object_latest_dates(
        obj_pattern_with_latest_dates: dict,
        object_pattern: str,
        file_modified_timestamp,
):
    new_obj_pattern_with_latest_dates = {
        **obj_pattern_with_latest_dates,
        object_pattern: convert_datetime_to_string(file_modified_timestamp)
    }
    return new_obj_pattern_with_latest_dates


def upload_s3_object_json(
        obj_pattern_with_latest_dates: dict,
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    upload_s3_object(
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object,
        data_object=json.dumps(obj_pattern_with_latest_dates)
    )


def get_initial_state(
        data_config: S3CsvConfig,
        latest_processed_file_date: str =
        DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
):
    return {
        object_name_pattern: latest_processed_file_date
        for object_name_pattern in data_config.s3_object_key_pattern_list
    }


def get_stored_state(
        data_config: S3CsvConfig
):
    try:
        downloaded_state = download_s3_json_object(
            data_config.state_file_bucket_name,
            data_config.state_file_object_name
        )
        stored_state = {
            object_pattern:
                downloaded_state.get(
                    object_pattern, DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
                )
            for object_pattern in data_config.s3_object_key_pattern_list
        }
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = get_initial_state(data_config)
        else:
            raise ex
    return {
        k: convert_datetime_string_to_datetime(v)
        for k, v in stored_state.items()
    }


def get_csv_data_from_s3(s3_bucket_name: str, s3_object_name: str):

    return download_s3_object_as_string(
        s3_bucket_name, s3_object_name
    )


def get_sorted_in_sheet_metadata_index(csv_config: S3CsvConfig):
    record_metadata = [
        {line_index_in_data: metadata_col_name}
        for metadata_col_name, line_index_in_data
        in csv_config.in_sheet_record_metadata.items()
    ]
    return sorted(
        record_metadata, key=lambda i: list(i.keys())
    )


def extend_table_if_new_col_exist(
        csv_config: S3CsvConfig,
        standardized_csv_header: list,
        record_metadata
):
    new_col_names = get_new_table_columns_schema(
        csv_config,
        standardized_csv_header,
        record_metadata
    )
    if new_col_names:
        extend_table_schema_field_names(
            csv_config.gcp_project,
            csv_config.dataset_name,
            csv_config.table_name,
            new_col_names,
        )


def get_record_metadata(
        record_list: list,
        csv_config: S3CsvConfig,
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


def get_standardized_csv_header(
        record_list: list,
        csv_config: S3CsvConfig
):
    csv_header = record_list[csv_config.header_line_index].split(",")
    standardized_csv_header = [
        standardize_field_name(field.lower()) for field in csv_header
    ]
    return standardized_csv_header


def get_csv_dict_reader(
        csv_string: str,
        standardized_csv_header: list,
        csv_config: S3CsvConfig
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


def transform_load_data(
        s3_object_name: str,
        csv_config: S3CsvConfig,
        record_import_timestamp_as_string: str,
        full_temp_file_location: str,
):
    LOGGER.info('processing object: "%s"', s3_object_name)
    csv_string = get_csv_data_from_s3(
        csv_config.s3_bucket_name, s3_object_name
    )
    record_list = csv_string.split("\n")
    record_metadata = get_record_metadata(
        record_list, csv_config, s3_object_name,
        record_import_timestamp_as_string
    )
    standardized_csv_header = get_standardized_csv_header(
        record_list, csv_config
    )
    auto_detect_schema = True
    if does_bigquery_table_exist(
            csv_config.gcp_project,
            csv_config.dataset_name,
            csv_config.table_name,
    ):
        extend_table_if_new_col_exist(
            csv_config,
            standardized_csv_header,
            record_metadata
        )
        auto_detect_schema = False

    csv_dict_reader = get_csv_dict_reader(
        csv_string,
        standardized_csv_header,
        csv_config
    )
    processed_record = process_record_list(
        csv_dict_reader, record_metadata,
    )

    write_load_processed_data_to_bq(
        processed_record,
        full_temp_file_location,
        auto_detect_schema,
        csv_config,
    )


def write_load_processed_data_to_bq(
        processed_record,
        full_temp_file_location,
        auto_detect_schema,
        csv_config,
):
    write_to_file(processed_record, full_temp_file_location)
    write_disposition = get_write_disposition(csv_config)

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=csv_config.table_name,
        auto_detect_schema=auto_detect_schema,
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
):
    for record in reader:
        n_record = merge_record_with_metadata(
            record=record,
            record_metadata=record_metadata,
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


def update_metadata_with_provenance(
        record_metadata, s3_bucket, s3_object
):
    provenance = {
        "s3_bucket": s3_bucket,
        "s3_object": s3_object
    }
    return {
        **record_metadata,
        "provenance": provenance
    }


class NamedLiterals:
    DAG_RUN = 'dag_run'
    RUN_ID = 'run_id'
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    S3_FILE_METADATA_LAST_MODIFIED_KEY = "LastModified"
    DEFAULT_AWS_CONN_ID = "aws_default"
