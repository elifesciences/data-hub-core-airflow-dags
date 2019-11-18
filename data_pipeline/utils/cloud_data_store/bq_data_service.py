"""
bq data service
written by tayowonibi
"""

import logging
import os
from math import ceil
from typing import List

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import NotFound

LOGGER = logging.getLogger(__name__)

MAX_ROWS_INSERTABLE = 1000


def load_file_into_bq(
        filename: str,
        dataset_name: str,
        table_name: str,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        rows_to_skip=0
):
    """
    :param filename:
    :param dataset_name:
    :param table_name:
    :param source_format:
    :param rows_to_skip:
    :param bq_ignore_unknown_values:
    :return:
    """
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info("File %s is empty.", filename)
        return
    client = Client()
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = LoadJobConfig()
    job_config.source_format = source_format
    if source_format is bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip

    with open(filename, "rb") as source_file:

        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config)

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info("Loaded %s rows into %s:%s.",
                    job.output_rows, dataset_name, table_name)


def load_tuple_list_into_bq(tuple_list_to_insert: List[tuple],
                            dataset_name: str, table_name: str) -> List[dict]:
    """
    :param tuple_list_to_insert:
    :param dataset_name:
    :param table_name:
    :return:
    """
    client = Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request

    errors = list()

    for indx in range(ceil(len(tuple_list_to_insert) / MAX_ROWS_INSERTABLE)):
        errors.extend(
            client.insert_rows(
                table,
                tuple_list_to_insert[indx * MAX_ROWS_INSERTABLE:
                                     (indx + 1) * MAX_ROWS_INSERTABLE],
            )
        )
    LOGGER.info("Loaded  data into %s:%s.", dataset_name, table_name)
    return errors


def create_table_if_not_exist(
        project_name: str, dataset_name: str,
        table_name: str, json_schema: dict
):
    """
    :param project_name:
    :param dataset_name:
    :param table_name:
    :param json_schema:
    :return:
    """
    try:
        if not does_bigquery_table_exist(
                project_name, dataset_name, table_name):
            client = bigquery.Client()
            table_id = compose_full_table_name(
                project_name, dataset_name, table_name)
            schema = get_schemafield_list_from_json_list(json_schema)
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table, True)  # API request
            LOGGER.info(
                (
                    "Created table %s.%s.%s",
                    table.project, table.dataset_id, table.table_id
                )
            )
    except OSError:
        LOGGER.error("Table Not Created")


def does_bigquery_table_exist(
        project_name: str,
        dataset_name: str,
        table_name: str) -> bool:
    """
    :param project_name:
    :param dataset_name:
    :param table_name:
    :return:
    """
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = bigquery.Client()
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def compose_full_table_name(
        project_name: str,
        dataset_name: str,
        table_name: str) -> str:
    """
    :param project_name:
    :param dataset_name:
    :param table_name:
    :return:
    """
    return ".".join([project_name, dataset_name, table_name])


def get_schemafield_list_from_json_list(json_schema: List[dict])\
        -> List[SchemaField]:
    """
    :param json_schema:
    :return:
    """
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema
