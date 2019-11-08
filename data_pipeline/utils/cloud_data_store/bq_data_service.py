import logging
from math import ceil
from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client
import os
LOGGER = logging.getLogger(__name__)

MAX_ROWS_INSERTABLE = 1000


def load_file_into_bq(
    filename: str,
    dataset_name: str,
    table_name: str,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    rows_to_skip=0,
    bq_ignore_unknown_values: bool = False
):
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info(
            "File {} is empty.".format(filename)
        )
        return
    client = Client()
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = LoadJobConfig()
    job_config.source_format = source_format
    job_config.ignore_unknown_values = bq_ignore_unknown_values
    if source_format is bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip

    with open(filename, "rb") as source_file:

        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config)

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info(
            "Loaded {} rows into {}:{}.".format(
                job.output_rows,
                dataset_name,
                table_name))


def load_tuple_list_into_bq(tuple_list_to_insert, dataset_name, table_name):
    client = Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request

    errors = list()

    for x in range(ceil(len(tuple_list_to_insert) / MAX_ROWS_INSERTABLE)):
        errors.extend(
            client.insert_rows(
                table,
                tuple_list_to_insert[
                    x * MAX_ROWS_INSERTABLE: (x + 1) * MAX_ROWS_INSERTABLE
                ],
            )
        )
    LOGGER.info("Loaded  data into {}:{}.".format(dataset_name, table_name))
    return errors


def create_table_if_not_exist(
    project_name: str, dataset_name: str, table_name: str, json_schema
):
    try:
        if not does_bigquery_table_exist(
                project_name, dataset_name, table_name):
            client = bigquery.Client()
            table_id = compose_full_table_name(
                project_name, dataset_name, table_name)
            schema = get_schema_from_json(json_schema)
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table, True)  # API request
            LOGGER.info(
                (
                    "Created table {}.{}.{}".format(
                        table.project, table.dataset_id, table.table_id
                    )
                )
            )
    except OSError:
        LOGGER.error("Table Not Created")


def does_bigquery_table_exist(
        project_name: str,
        dataset_name: str,
        table_name: str):
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = bigquery.Client()
    try:
        client.get_table(table_id)
        return True
    except BaseException:
        return False


def compose_full_table_name(
        project_name: str,
        dataset_name: str,
        table_name: str):
    return ".".join([project_name, dataset_name, table_name])


def get_schema_from_json(json_schema):
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema
