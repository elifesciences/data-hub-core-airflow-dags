import json
import re
from datetime import timezone, datetime

from botocore.exceptions import ClientError
from dateutil import tz
from google.cloud.bigquery import WriteDisposition

from data_pipeline.s3_csv_data.s3_csv_config import S3CsvConfig
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    load_file_into_bq,
    get_table_schema_field_names,
    extend_table_schema_field_names,
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
    upload_s3_object
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string
)

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
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    latest_obj_timestamp = {
        object_pattern: convert_datetime_to_string(file_modified_timestamp)
    }
    obj_pattern_with_latest_dates.update(latest_obj_timestamp)

    upload_s3_object(
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object,
        data_object=json.dumps(obj_pattern_with_latest_dates)
    )


def init_state_file(
        data_config: S3CsvConfig,
        latest_processed_file_date: str =
        DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
):
    obj_pattern_with_latest_dates = dict()
    for object_name_pattern in data_config.s3_object_key_pattern_list:
        obj_pattern_with_latest_dates[object_name_pattern] = (
            latest_processed_file_date
        )
    return obj_pattern_with_latest_dates


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
            stored_state = init_state_file(data_config)
        else:
            raise ex
    return {
        k: convert_datetime_string_to_datetime(v)
        for k, v in stored_state.items()
    }


def get_csv_data_from_s3(s3_bucket_name: str, s3_object_name: str):
    csv_string = download_s3_object_as_string(
        s3_bucket_name, s3_object_name
    )
    return csv_string.split("\n")


def transform_load_data(
        s3_object_name: str,
        csv_config: S3CsvConfig,
        record_import_timestamp_as_string: str,
        full_temp_file_location: str,
):
    record_list = get_csv_data_from_s3(
        csv_config.s3_bucket_name, s3_object_name
    )
    record_metadata = {
        metadata_col_name: record_list[line_index_in_data]
        for metadata_col_name, line_index_in_data
        in csv_config.metadata.items()
    }

    record_metadata[
        csv_config.import_timestamp_field_name
    ] = record_import_timestamp_as_string

    record_metadata.update(csv_config.fixed_sheet_metadata)
    record_metadata = update_metadata_with_provenance(
        record_metadata, csv_config.s3_bucket_name, s3_object_name
    )

    csv_header = record_list[csv_config.header_line_index].split(",")
    standardized_csv_header = [
        standardize_field_name(field.lower()) for field in csv_header
    ]

    auto_detect_schema = True
    if does_bigquery_table_exist(
            csv_config.gcp_project,
            csv_config.dataset_name,
            csv_config.table_name,
    ):
        extend_table_schema(
            csv_config,
            standardized_csv_header,
            record_metadata
        )
        auto_detect_schema = False

    processed_record = process_record_list(
        record_list[csv_config.data_values_start_line_index:],
        record_metadata,
        standardized_csv_header,
    )

    write_to_file(processed_record, full_temp_file_location)
    write_disposition = (
        WriteDisposition.WRITE_APPEND
        if csv_config.table_write_append
        else WriteDisposition.WRITE_TRUNCATE
    )

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=csv_config.table_name,
        auto_detect_schema=auto_detect_schema,
        dataset_name=csv_config.dataset_name,
        write_mode=write_disposition,
        project_name=csv_config.gcp_project,
    )


def strip_quotes_from_csv_cell_value(csv_cell_value: str):
    csv_cell_value = csv_cell_value.strip()
    csv_cell_value = (
        csv_cell_value.strip("\"")
        if (csv_cell_value.startswith("\"") and csv_cell_value.endswith("\""))
        else csv_cell_value
    )
    csv_cell_value = (
        csv_cell_value.strip("'")
        if (csv_cell_value.startswith("'") and csv_cell_value.endswith("'"))
        else csv_cell_value
    )
    return csv_cell_value


def write_to_file(json_list, full_temp_file_location: str):
    with open(full_temp_file_location, "a") as write_file:
        for record in json_list:
            write_file.write(json.dumps(record, ensure_ascii=False))
            write_file.write("\n")


def process_record_list(
        record_list: list,
        record_metadata: dict,
        standardized_csv_header: list
):
    for record in record_list:
        if record.strip() == "":
            n_record = process_record(
                record=record,
                record_metadata=record_metadata,
                standardized_csv_header=standardized_csv_header,
            )
            yield n_record


def process_record(record: str,
                   record_metadata: dict,
                   standardized_csv_header: list
                   ):
    returned_record = record_metadata
    record_as_list = record.split(",")
    record_length = len(record_as_list)
    record_as_dict = {
        field_name: strip_quotes_from_csv_cell_value(
            record_as_list[record_col_index]
        )
        for record_col_index, field_name in enumerate(standardized_csv_header)
        if record_col_index < record_length
    }
    returned_record.update(record_as_dict)
    return returned_record


def update_metadata_with_provenance(
        record_metadata, s3_bucket, s3_object
):
    provenance = {
        "s3_bucket": s3_bucket,
        "s3_object": s3_object
    }
    record_metadata["provenance"] = provenance
    return record_metadata


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", strip_quotes_from_csv_cell_value(field_name))


def extend_table_schema(
        csv_config: S3CsvConfig,
        standardized_csv_header: list,
        record_metadata: dict
):
    existing_table_field_names = get_table_schema_field_names(
        project_name=csv_config.gcp_project,
        dataset_name=csv_config.dataset_name,
        table_name=csv_config.table_name,
    )
    existing_table_field_names = [
        f_name.lower() for f_name in existing_table_field_names
    ]

    new_table_field_names = standardized_csv_header[:]
    new_table_field_names.extend(
        [key.lower() for key in record_metadata.keys()]
    )
    existing_table_field_names_set = set(existing_table_field_names)
    new_table_field_name_set = set(new_table_field_names)

    new_col_names = list(
        new_table_field_name_set - existing_table_field_names_set
    )

    if len(new_col_names) > 0:
        extend_table_schema_field_names(
            csv_config.gcp_project,
            csv_config.dataset_name,
            csv_config.table_name,
            new_col_names,
        )
