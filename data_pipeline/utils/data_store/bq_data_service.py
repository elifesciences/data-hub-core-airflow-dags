import logging
import os
from math import ceil
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client, table as bq_table
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import WriteDisposition

LOGGER = logging.getLogger(__name__)

MAX_ROWS_INSERTABLE = 1000


# pylint: disable=too-many-arguments
def load_file_into_bq(
        filename: str,
        dataset_name: str,
        table_name: str,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_mode=WriteDisposition.WRITE_APPEND,
        auto_detect_schema=False,
        rows_to_skip=0,
        project_name: str = None,
):
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info("File %s is empty.", filename)
        return
    client = Client(project=project_name) if project_name else Client()
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = LoadJobConfig()
    job_config.write_disposition = write_mode
    job_config.autodetect = auto_detect_schema
    job_config.source_format = source_format
    if source_format is bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config
        )

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info(
            "Loaded %s rows into %s:%s.",
            job.output_rows,
            dataset_name,
            table_name
        )


def load_tuple_list_into_bq(
        tuple_list_to_insert: List[tuple], dataset_name: str, table_name: str
) -> List[dict]:
    client = Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request

    errors = list()

    for indx in range(ceil(len(tuple_list_to_insert) / MAX_ROWS_INSERTABLE)):
        errors.extend(
            load_tuple_list_page_into_bq(
                client,
                table,
                tuple_list_to_insert[
                    indx * MAX_ROWS_INSERTABLE:
                    (indx + 1) * MAX_ROWS_INSERTABLE
                ],
            )
        )
    LOGGER.info("Loaded  data into %s:%s.", dataset_name, table_name)
    return errors


def load_tuple_list_page_into_bq(
        client: Client, table: bq_table, tuple_list_to_insert: List[tuple],
) -> List[dict]:

    errors = client.insert_rows(table, tuple_list_to_insert)
    return errors


def create_table_if_not_exist(
        project_name: str,
        dataset_name: str,
        table_name: str,
        json_schema: dict
):

    if not does_bigquery_table_exist(project_name, dataset_name, table_name):
        client = bigquery.Client()
        table_id = compose_full_table_name(
            project_name, dataset_name, table_name
        )
        schema = get_schemafield_list_from_json_list(json_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, True)  # API request
        LOGGER.info(
            "Created table %s.%s.%s",
            table.project,
            table.dataset_id,
            table.table_id
        )


def does_bigquery_table_exist(
        project_name: str, dataset_name: str, table_name: str
) -> bool:
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = bigquery.Client()
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def compose_full_table_name(
        project_name: str, dataset_name: str, table_name: str
) -> str:
    return ".".join([project_name, dataset_name, table_name])


def get_schemafield_list_from_json_list(
        json_schema: List[dict]
) -> List[SchemaField]:
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema


def get_table_schema_field_names(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)  # API Request
        return [field.name for field in table.schema]
    except NotFound:
        return []


def extend_table_schema_field_names(
        project_name: str, dataset_name: str,
        table_name: str, new_field_names: list
):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)  # Make an API request.
    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    for field in new_field_names:
        new_schema.append(bigquery.SchemaField(field, "STRING"))

    table.schema = new_schema
    client.update_table(table, ["schema"])  # Make an API request.
