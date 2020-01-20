import datetime
import json
import re
from datetime import timezone
from tempfile import NamedTemporaryFile

import yaml
from google.cloud.bigquery import WriteDisposition

from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet,
    CsvSheetConfig,
)
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    load_file_into_bq,
    get_table_schema_field_names,
    extend_table_schema_field_names,
)
from data_pipeline.utils.data_store.google_spreadsheet_service import (
    download_google_spreadsheet_single_sheet,
)


def current_timestamp_as_string():
    dtobj = datetime.datetime.now(timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")


def etl_google_spreadsheet(spreadsheet_config: MultiCsvSheet):
    for _, csv_sheet_config in spreadsheet_config.sheets_config.items():
        with NamedTemporaryFile() as named_temp_file:
            process_csv_sheet(csv_sheet_config, named_temp_file.name)


def process_csv_sheet(
        csv_sheet_config: CsvSheetConfig, temp_file: str,
):
    sheet_with_range = (
        csv_sheet_config.sheet_name + "!" + csv_sheet_config.sheet_range
        if csv_sheet_config.sheet_range
        else csv_sheet_config.sheet_name
    )
    downloaded_data = download_google_spreadsheet_single_sheet(
        csv_sheet_config.spreadsheet_id, sheet_with_range
    )
    record_import_timestamp_as_string = current_timestamp_as_string()
    transform_load_data(
        record_list=downloaded_data,
        csv_sheet_config=csv_sheet_config,
        record_import_timestamp_as_string=record_import_timestamp_as_string,
        full_temp_file_location=temp_file,
    )


def extend_table_schema(
        csv_sheet_config: CsvSheetConfig,
        standardized_csv_header: list,
        record_metadata: dict
):
    existing_table_field_names = get_table_schema_field_names(
        project_name=csv_sheet_config.gcp_project,
        dataset_name=csv_sheet_config.dataset_name,
        table_name=csv_sheet_config.table_name,
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
            csv_sheet_config.gcp_project,
            csv_sheet_config.dataset_name,
            csv_sheet_config.table_name,
            new_col_names,
        )


def update_metadata_with_provenance(
        record_metadata, csv_sheet_config: CsvSheetConfig
):
    provenance = {
        "spreadsheet_id": csv_sheet_config.spreadsheet_id,
        "sheet_name": csv_sheet_config.sheet_name,
    }
    record_metadata["provenance"] = provenance
    return record_metadata


def transform_load_data(
        record_list,
        csv_sheet_config: CsvSheetConfig,
        record_import_timestamp_as_string: str,
        full_temp_file_location: str,
):
    record_metadata = {
        metadata_col_name: ",".join(record_list[line_index_in_data])
        for metadata_col_name, line_index_in_data
        in csv_sheet_config.metadata.items()
    }
    record_metadata[
        csv_sheet_config.import_timestamp_field_name
    ] = record_import_timestamp_as_string
    record_metadata.update(csv_sheet_config.fixed_sheet_metadata)
    record_metadata = update_metadata_with_provenance(
        record_metadata, csv_sheet_config
    )

    csv_header = record_list[csv_sheet_config.header_line_index]
    standardized_csv_header = [
        standardize_field_name(field.lower()) for field in csv_header
    ]

    auto_detect_schema = True
    if does_bigquery_table_exist(
            csv_sheet_config.gcp_project,
            csv_sheet_config.dataset_name,
            csv_sheet_config.table_name,
    ):
        extend_table_schema(
            csv_sheet_config,
            standardized_csv_header,
            record_metadata
        )
        auto_detect_schema = False

    processed_record = process_record_list(
        record_list[csv_sheet_config.data_values_start_line_index:],
        record_metadata,
        standardized_csv_header,
    )

    write_to_file(processed_record, full_temp_file_location)
    write_disposition = (
        WriteDisposition.WRITE_APPEND
        if csv_sheet_config.table_write_append
        else WriteDisposition.WRITE_TRUNCATE
    )

    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=csv_sheet_config.table_name,
        auto_detect_schema=auto_detect_schema,
        dataset_name=csv_sheet_config.dataset_name,
        write_mode=write_disposition,
        project_name=csv_sheet_config.gcp_project,
    )


def write_to_file(json_list: list, full_temp_file_location: str):
    with open(full_temp_file_location, "a") as write_file:
        for record in json_list:
            write_file.write(json.dumps(record, ensure_ascii=False))
            write_file.write("\n")


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)


def process_record(record: list,
                   record_metadata: dict,
                   standardized_csv_header: list
                   ):
    record_length = len(record)
    record_as_dict = {
        field_name: record[record_col_index]
        for record_col_index, field_name in enumerate(standardized_csv_header)
        if record_col_index < record_length
    }
    record_as_dict.update(record_metadata)
    return record_as_dict


def process_record_list(
        record_list: list,
        record_metadata: dict,
        standardized_csv_header: list
):
    for record in record_list:
        n_record = process_record(
            record=record,
            record_metadata=record_metadata,
            standardized_csv_header=standardized_csv_header,
        )
        yield n_record


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)
