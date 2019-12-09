import json
import datetime
from datetime import timezone
from tempfile import NamedTemporaryFile
from google.cloud.bigquery import WriteDisposition
from data_pipeline.spreadsheet_data.transform_spreadsheet import get_col_index_to_field_name_mapping, process_record_list
from data_pipeline.spreadsheet_data.google_spreadsheet_config import MultiCsvSheet
from data_pipeline.utils.data_store.google_spreadsheet_service import download_google_spreadsheet_single_sheet
from data_pipeline.utils.data_store.s3_data_service import download_s3_json_object
from data_pipeline.utils.data_store.bq_data_service import load_file_into_bq


def current_timestamp_as_string():
    dtobj = datetime.datetime.now(timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")


def etl_google_spreadsheet(
    spreadsheet_config: dict
):
    for sheet_name, _ in spreadsheet_config.sheets_config.items():
        with NamedTemporaryFile() as named_temp_file:
            process_csv_sheet(sheet_name, spreadsheet_config, named_temp_file.name)


def process_csv_sheet(sheet_name, spreadsheet_config: MultiCsvSheet, temp_file: str,):
    spreadsheet_property = spreadsheet_config.sheets_config.get(sheet_name)
    sheet_with_range = (
        spreadsheet_property.sheet_name
        + "!"
        + spreadsheet_property.sheet_range
        if spreadsheet_property.sheet_range
        else spreadsheet_property.sheet_name
    )
    downloaded_data = download_google_spreadsheet_single_sheet(spreadsheet_config.spreadsheet_id, sheet_with_range)

    json_schema = download_s3_json_object(
        bucket=spreadsheet_property.schema_bucket,
        object_key=spreadsheet_property.schema_bucket_object_name
    )
    record_import_timestamp_as_string = current_timestamp_as_string()
    transform_load_data(record_list=downloaded_data,
                        sheet_name=sheet_name,
                        spreadsheet_config=spreadsheet_config,
                        record_import_timestamp_as_string=record_import_timestamp_as_string,
                        full_temp_file_location=temp_file,
                        json_schema=json_schema
                        )


def transform_load_data(record_list, sheet_name, spreadsheet_config: MultiCsvSheet,
                        record_import_timestamp_as_string: str, full_temp_file_location: str,
                        json_schema: dict):

    sheet_config = spreadsheet_config.sheets_config.get(sheet_name)

    schema_field_names = [
        element.get("name")
        for element in json_schema
    ]
    schema_mapping = sheet_config.header_mapping_to_schema
    csv_header = record_list[sheet_config.header_line_index]

    col_index_to_field_name_mapping = get_col_index_to_field_name_mapping(
        csv_header=csv_header,
        schema_field_names=schema_field_names,
        partial_schema_mapping=schema_mapping
    )

    record_metadata = {
        metadata_col_name:
        ",".join(record_list[line_index_in_data])
        for metadata_col_name, line_index_in_data in sheet_config.metadata.items()
    }
    record_metadata[spreadsheet_config.import_timestamp_field_name] =\
        record_import_timestamp_as_string
    record_metadata.update(sheet_config.fixed_sheet_metadata)
    processed_record =\
        process_record_list(record_list[sheet_config.data_values_start_line_index:], record_metadata,
                            col_index_to_field_name_mapping)
    write_to_file(processed_record, full_temp_file_location)
    write_disposition = (
        WriteDisposition.WRITE_APPEND
        if sheet_config.table_write_append
        else WriteDisposition.WRITE_TRUNCATE
    )

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=sheet_config.table_name,
        auto_detect_schema=False,
        dataset_name=sheet_config.dataset_name,
        write_mode=write_disposition,
        project_name=spreadsheet_config.gcp_project
    )


def write_to_file(json_list, full_temp_file_location):
    with open(full_temp_file_location, "a") as write_file:
        for record in json_list:
            write_file.write(json.dumps(record, ensure_ascii=False))
            write_file.write("\n")
