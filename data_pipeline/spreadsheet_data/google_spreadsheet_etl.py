import re
from tempfile import NamedTemporaryFile

from google.cloud.bigquery import WriteDisposition

from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet,
    BaseCsvSheetConfig,
)
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    load_file_into_bq,
)
from data_pipeline.utils.data_store.google_spreadsheet_service import (
    download_google_spreadsheet_single_sheet,
)

from data_pipeline.utils.csv.metadata_schema import (
    extend_nested_table_schema_if_new_fields_exist,
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string
)
from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file


def etl_google_spreadsheet(spreadsheet_config: MultiCsvSheet):
    current_timestamp_as_str = get_current_timestamp_as_string()
    for csv_sheet_config in spreadsheet_config.sheets_config.values():
        with NamedTemporaryFile() as named_temp_file:
            process_csv_sheet(
                csv_sheet_config,
                named_temp_file.name,
                current_timestamp_as_str
            )


def get_sheet_range_from_config(
        csv_sheet_config: BaseCsvSheetConfig
):
    sheet_with_range = (
        csv_sheet_config.sheet_name + "!" + csv_sheet_config.sheet_range
        if csv_sheet_config.sheet_range
        else csv_sheet_config.sheet_name
    )
    return sheet_with_range


def process_csv_sheet(
        csv_sheet_config: BaseCsvSheetConfig, temp_file: str,
        timestamp_as_string: str
):
    sheet_with_range = get_sheet_range_from_config(csv_sheet_config)
    downloaded_data = download_google_spreadsheet_single_sheet(
        csv_sheet_config.spreadsheet_id, sheet_with_range
    )
    record_import_timestamp_as_string = timestamp_as_string
    transform_load_data(
        record_list=downloaded_data,
        csv_sheet_config=csv_sheet_config,
        record_import_timestamp_as_string=record_import_timestamp_as_string,
        full_temp_file_location=temp_file,
    )


def update_metadata_with_provenance(
        record_metadata, csv_sheet_config: BaseCsvSheetConfig
):
    provenance = {
        NamedLiterals.PROVENANCE_SPREADSHEET_ID:
            csv_sheet_config.spreadsheet_id,
        NamedLiterals.PROVENANCE_SHEET_NAME:
            csv_sheet_config.sheet_name,
    }
    return {
        **record_metadata,
        "provenance": provenance
    }


def get_record_metadata(
        record_list,
        csv_sheet_config: BaseCsvSheetConfig,
        record_import_timestamp_as_string: str,
):
    record_metadata = {
        metadata_col_name: ",".join(record_list[line_index_in_data])
        for metadata_col_name, line_index_in_data
        in csv_sheet_config.in_sheet_record_metadata.items()
    }
    record_metadata[
        csv_sheet_config.import_timestamp_field_name
    ] = record_import_timestamp_as_string
    record_metadata.update(csv_sheet_config.fixed_sheet_record_metadata)
    record_metadata = update_metadata_with_provenance(
        record_metadata, csv_sheet_config
    )
    return record_metadata


def get_standardized_csv_header(csv_header):
    return [
        standardize_field_name(field.lower())
        for field in csv_header
    ]


def get_write_disposition(csv_sheet_config):
    write_disposition = (
        WriteDisposition.WRITE_APPEND
        if csv_sheet_config.table_write_append_enabled
        else WriteDisposition.WRITE_TRUNCATE
    )
    return write_disposition


def transform_load_data(
        record_list,
        csv_sheet_config: BaseCsvSheetConfig,
        record_import_timestamp_as_string: str,
        full_temp_file_location: str,
):

    record_metadata = get_record_metadata(
        record_list,
        csv_sheet_config,
        record_import_timestamp_as_string
    )

    csv_header = record_list[csv_sheet_config.header_line_index]
    standardized_csv_header = get_standardized_csv_header(
        csv_header
    )

    auto_detect_schema = True
    if does_bigquery_table_exist(
            csv_sheet_config.gcp_project,
            csv_sheet_config.dataset_name,
            csv_sheet_config.table_name,
    ):
        provenance_schema = (
            google_spreadsheet_csv_provenance_schema()
        )
        extend_nested_table_schema_if_new_fields_exist(
            standardized_csv_header,
            csv_sheet_config,
            provenance_schema
        )
        auto_detect_schema = False

    processed_record = process_record_list(
        record_list[csv_sheet_config.data_values_start_line_index:],
        record_metadata,
        standardized_csv_header,
    )
    write_jsonl_to_file(processed_record, full_temp_file_location)
    write_disposition = get_write_disposition(csv_sheet_config)
    load_file_into_bq(
        filename=full_temp_file_location,
        table_name=csv_sheet_config.table_name,
        auto_detect_schema=auto_detect_schema,
        dataset_name=csv_sheet_config.dataset_name,
        write_mode=write_disposition,
        project_name=csv_sheet_config.gcp_project,
    )


def standardize_field_name(field_name: str):
    return re.sub(r"\W", "_", field_name.strip().strip('"').strip("'"))


def process_record(record: list,
                   record_metadata: dict,
                   standardized_csv_header: list
                   ):
    return {
        **record_metadata,
        **dict(zip(standardized_csv_header, record))
    }


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


def google_spreadsheet_csv_provenance_schema():
    prov_dict = {
        "name": NamedLiterals.PROVENANCE_FIELD_NAME,
        "type": "RECORD",
        "fields": [
            {
                "name":
                    NamedLiterals.PROVENANCE_SHEET_NAME,
                "type": "STRING"
            },
            {
                "name":
                    NamedLiterals.PROVENANCE_SPREADSHEET_ID,
                "type": "STRING"
            },
        ]
    }
    prov_schema_list = [prov_dict]
    return prov_schema_list


class NamedLiterals:
    PROVENANCE_FIELD_NAME = "provenance"
    PROVENANCE_SHEET_NAME = "sheet_name"
    PROVENANCE_SPREADSHEET_ID = "spreadsheet_id"
