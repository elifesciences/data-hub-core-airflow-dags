from data_pipeline.utils.data_store.bq_data_service import (
    extend_table_schema_with_nested_schema
)


def convert_header_list_to_bigquery_schema(
        standardized_csv_header: list,
        csv_sheet_config,
):
    bigquery_schema = [
        {
            "type": "STRING",
            "name": col_name
        } for col_name in standardized_csv_header
        if col_name.lower() != csv_sheet_config.import_timestamp_field_name
    ]
    if csv_sheet_config.import_timestamp_field_name in standardized_csv_header:
        bigquery_schema.append(
            {
                "type": "TIMESTAMP",
                "name": csv_sheet_config.import_timestamp_field_name
            }
        )
    return bigquery_schema


def get_record_metadata_schema(
        csv_config,
):
    rec_meta_schema = [
        {
            "name": key_name,
            "type": "STRING"
        }
        for key_name in
        list(csv_config.fixed_sheet_record_metadata.keys()) +
        list(csv_config.in_sheet_record_metadata.keys())
    ]

    return rec_meta_schema


def get_record_metadata_with_provenance_schema(
        csv_config,
        provenance_schema,
):
    return [
        *(get_record_metadata_schema(csv_config)),
        *provenance_schema
    ]


def extend_nested_table_schema_if_new_fields_exist(
        csv_header: list,
        csv_config,
        provenance_schema
):
    bq_schema = convert_header_list_to_bigquery_schema(
        csv_header, csv_config
    )
    bq_schema.extend(
        get_record_metadata_with_provenance_schema(
            csv_config,
            provenance_schema=provenance_schema
        )
    )

    extend_table_schema_with_nested_schema(
        csv_config.gcp_project,
        csv_config.dataset_name,
        csv_config.table_name,
        bq_schema,
    )
