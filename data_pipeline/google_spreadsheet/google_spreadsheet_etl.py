import logging
import re
from tempfile import TemporaryDirectory
from pathlib import Path
from typing import Iterable, Literal
from typing_extensions import TypedDict, NotRequired

from google.cloud.bigquery import WriteDisposition

from data_pipeline.google_spreadsheet.google_spreadsheet_config import (
    MultiCsvSheetConfig,
    BaseCsvSheetConfig,
)
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    load_file_into_bq,
    load_given_json_list_data_from_tempdir_to_bq,
)
from data_pipeline.utils.data_store.google_spreadsheet_service import (
    download_google_spreadsheet_single_sheet,
    get_spreadsheet_modified_timestamp_as_string,
)

from data_pipeline.utils.csv.metadata_schema import (
    extend_nested_table_schema_if_new_fields_exist,
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string,
    parse_timestamp
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    upload_s3_object
)
from data_pipeline.utils.pipeline_config import get_deployment_env
from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file


LOGGER = logging.getLogger(__name__)


class NamedLiterals:
    PROVENANCE_FIELD_NAME = 'provenance'
    PROVENANCE_SHEET_NAME = 'sheet_name'
    PROVENANCE_SPREADSHEET_ID = 'spreadsheet_id'


def update_metadata_with_provenance(
    record_metadata,
    csv_sheet_config: BaseCsvSheetConfig
):
    provenance = {
        NamedLiterals.PROVENANCE_SPREADSHEET_ID: csv_sheet_config.spreadsheet_id,
        NamedLiterals.PROVENANCE_SHEET_NAME: csv_sheet_config.sheet_name,
    }
    return {
        **record_metadata,
        'provenance': provenance
    }


def get_record_metadata(
    record_list,
    csv_sheet_config: BaseCsvSheetConfig,
    record_import_timestamp_as_string: str
):
    record_metadata = {
        metadata_col_name: ','.join(record_list[line_index_in_data])
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


def standardize_field_name(field_name: str):
    return re.sub(r'\W', '_', field_name.strip().strip('"').strip("'"))


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


def process_record(
    record: list,
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
        'name': NamedLiterals.PROVENANCE_FIELD_NAME,
        'type': 'RECORD',
        'fields': [
            {
                'name': NamedLiterals.PROVENANCE_SHEET_NAME,
                'type': 'STRING'
            },
            {
                'name': NamedLiterals.PROVENANCE_SPREADSHEET_ID,
                'type': 'STRING'
            },
        ]
    }
    prov_schema_list = [prov_dict]
    return prov_schema_list


def should_autodetect_schema(
    csv_sheet_config: BaseCsvSheetConfig,
    standardized_csv_header: list
):
    auto_detect_schema = True
    if does_bigquery_table_exist(
        csv_sheet_config.gcp_project,
        csv_sheet_config.dataset_name,
        csv_sheet_config.table_name,
    ):
        provenance_schema = google_spreadsheet_csv_provenance_schema()
        extend_nested_table_schema_if_new_fields_exist(
            standardized_csv_header,
            csv_sheet_config,
            provenance_schema
        )
        auto_detect_schema = False
    return auto_detect_schema


class DataHubPipelineMonitoringTableRowDict(TypedDict):
    pipeline_type: str
    data_pipeline_id: str
    table_name: str
    run_timestamp: str
    status: Literal['success', 'failure', 'skipped']
    error_message: NotRequired[str]


def append_to_data_hub_pipeline_monitoring_table(
    json_list: Iterable[DataHubPipelineMonitoringTableRowDict],
    project_name: str
):
    deployment_env = get_deployment_env()
    LOGGER.info(
        'Appending to data_hub_pipeline_monitoring table in %s.%s.data_hub_pipeline_monitoring',
        project_name,
        deployment_env
    )

    load_given_json_list_data_from_tempdir_to_bq(
        json_list=json_list,
        project_name=project_name,
        dataset_name=deployment_env,
        table_name='data_hub_pipeline_monitoring'
    )


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
    standardized_csv_header = get_standardized_csv_header(csv_header)

    LOGGER.info(
        'Loading data into BigQuery table: %s.%s',
        csv_sheet_config.dataset_name,
        csv_sheet_config.table_name
    )

    auto_detect_schema = should_autodetect_schema(
        csv_sheet_config,
        standardized_csv_header
    )

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


def get_sheet_range_from_config(
    csv_sheet_config: BaseCsvSheetConfig
):
    sheet_with_range = (
        csv_sheet_config.sheet_name + '!' + csv_sheet_config.sheet_range
        if csv_sheet_config.sheet_range
        else csv_sheet_config.sheet_name
    )
    return sheet_with_range


def process_csv_sheet(
    csv_sheet_config: BaseCsvSheetConfig,
    temp_file: str,
    timestamp_as_string: str
):
    spreadsheet_id = csv_sheet_config.spreadsheet_id
    sheet_with_range = get_sheet_range_from_config(csv_sheet_config)
    downloaded_data = download_google_spreadsheet_single_sheet(
        spreadsheet_id,
        sheet_with_range
    )
    record_import_timestamp_as_string = timestamp_as_string
    transform_load_data(
        record_list=downloaded_data,
        csv_sheet_config=csv_sheet_config,
        record_import_timestamp_as_string=record_import_timestamp_as_string,
        full_temp_file_location=temp_file,
    )


def etl_google_spreadsheet(spreadsheet_config: MultiCsvSheetConfig):
    LOGGER.info('spreadsheet_config: %r', spreadsheet_config)
    spreadsheet_id = spreadsheet_config.spreadsheet_id
    spreadsheet_modified_timestamp = parse_timestamp(
        get_spreadsheet_modified_timestamp_as_string(
            spreadsheet_id
        )
    )
    LOGGER.info(
        'Spreadsheet ID: %r, modified time: %r',
        spreadsheet_id,
        spreadsheet_modified_timestamp.isoformat()
    )
    current_timestamp_as_str = get_current_timestamp_as_string()
    if spreadsheet_config.state_file:
        try:
            state_timestamp = parse_timestamp(
                download_s3_object_as_string_or_file_not_found_error(
                    bucket=spreadsheet_config.state_file.bucket_name,
                    object_key=spreadsheet_config.state_file.object_name
                )
            )
            LOGGER.info(
                'State file s3://%s/%s has timestamp: %s',
                spreadsheet_config.state_file.bucket_name,
                spreadsheet_config.state_file.object_name,
                state_timestamp.isoformat()
            )
            if spreadsheet_modified_timestamp <= state_timestamp:
                LOGGER.info(
                    'No changes detected in spreadsheet since last ETL run. Exiting ETL process.'
                )
                append_to_data_hub_pipeline_monitoring_table(
                    [
                        {
                            'pipeline_type': 'google_spreadsheet',
                            'data_pipeline_id': spreadsheet_config.data_pipeline_id,
                            'table_name': sheet_config.table_name,
                            'run_timestamp': current_timestamp_as_str,
                            'status': 'skipped'
                        }
                        for _, sheet_config in spreadsheet_config.sheets_config.items()
                    ],
                    project_name=spreadsheet_config.gcp_project
                )
                return
        except FileNotFoundError:
            LOGGER.warning(
                'State file s3://%s/%s not found. Continuing without state.',
                spreadsheet_config.state_file.bucket_name,
                spreadsheet_config.state_file.object_name
            )
    for csv_sheet_config in spreadsheet_config.sheets_config.values():
        with TemporaryDirectory() as tmp_dir:
            full_temp_file_location = str(
                Path(tmp_dir, 'downloaded_jsonl_data')
            )
            process_csv_sheet(
                csv_sheet_config,
                full_temp_file_location,
                current_timestamp_as_str
            )
    if spreadsheet_config.state_file:
        LOGGER.info(
            'Updating state file s3://%s/%s with timestamp: %s',
            spreadsheet_config.state_file.bucket_name,
            spreadsheet_config.state_file.object_name,
            spreadsheet_modified_timestamp
        )
        upload_s3_object(
            bucket=spreadsheet_config.state_file.bucket_name,
            object_key=spreadsheet_config.state_file.object_name,
            data_object=spreadsheet_modified_timestamp.isoformat()
        )
