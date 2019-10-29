import logging
from math import ceil
from typing import List
from pathlib import Path
from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery
from pandas import DataFrame

import os
LOGGER = logging.getLogger(__name__)
MAX_ROWS_INSERTABLE = 1000


UPLOAD_MESSAGE_INDEX = 'index'
UPLOAD_MESSAGE_ERRORS = 'errors'
UPLOAD_MESSAGE_MESSAGE = 'message'
UPLOAD_MESSAGE_REASON = 'reason'
UPLOAD_MESSAGE_STOPPED = 'stopped'

def load_file_into_bq(
    filename: str,
    dataset_name: str,
    table_name: str,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    rows_to_skip=0,
):
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info(
            "File {} is empty.".format(filename)
        )
        return
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = source_format
    if source_format is bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table cloud_data_store to complete
    LOGGER.info(
        "Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name)
    )


def load_tuple_list_into_bq(tuple_list_to_insert, dataset_name, table_name):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request

    errors = list()

    for x in range(ceil(len(tuple_list_to_insert) / MAX_ROWS_INSERTABLE)):
        errors.extend(
            client.insert_rows(
                table,
                tuple_list_to_insert[
                    x * MAX_ROWS_INSERTABLE : (x + 1) * MAX_ROWS_INSERTABLE
                ],
            )
        )

    LOGGER.info("Loaded  data into {}:{}.".format(dataset_name, table_name))

    return errors


def load_json_list_to_bq_single_pass(json_data, bq_client, table):

    errors = bq_client.insert_rows_json(table, json_data)
    if len(errors) > 0:
        error_list = set([row_message.get(UPLOAD_MESSAGE_INDEX) for row_message in errors if
                          row_message.get(UPLOAD_MESSAGE_ERRORS)[0].get(UPLOAD_MESSAGE_MESSAGE) != '' or row_message.get(UPLOAD_MESSAGE_ERRORS)[0].get(
                              UPLOAD_MESSAGE_REASON) != UPLOAD_MESSAGE_STOPPED])
        error_message = [errors[index] for index in error_list]
        error_rows = [json_data[index] for index in error_list]
        insertable_rows = [json_data[indx] for indx in range(0, len(json_data)) if indx not in error_list]
        bq_client.insert_rows_json(table, insertable_rows)
        return error_rows, error_message
    return [], []


def load_json_list_into_bq(json_list_to_insert, dataset_name, table_name):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request
    uninserted_row_message_list = list()
    uninserted_row_list = list()
    for x in range(ceil(len(json_list_to_insert) / MAX_ROWS_INSERTABLE)):
        data_rows, messages = load_json_list_to_bq_single_pass(json_list_to_insert[
                    x * MAX_ROWS_INSERTABLE : (x + 1) * MAX_ROWS_INSERTABLE
                ], client, table)
        uninserted_row_message_list.extend(messages)
        uninserted_row_list.extend(data_rows)

    LOGGER.info("Loaded  data into {}:{}.".format(dataset_name, table_name))

    return uninserted_row_list, uninserted_row_message_list


def load_pandas_data_frame_into_bq(
    data_frame: DataFrame, dataset_name: str, table_name: str
):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request
    errors = client.load_table_from_dataframe(data_frame, table)

    LOGGER.info("Loaded  data into {}:{}.".format(dataset_name, table_name))

    return errors


def get_table_schema(source_schema_file: str) -> List:
    try:

        client = bigquery.Client()
        schema = client.schema_from_json(source_schema_file)
        return schema
    except OSError as e:
        LOGGER.error(
            "File not found  : {}, error message {}".format(source_schema_file, e)
        )
    except KeyError as e:
        LOGGER.error("Json does not contain required key named :  {}".format(e))


def get_schema_from_json(json_schema):
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema


def get_table_schema_file_name(project_name: str, dataset: str, table_name: str, schema_dir:str) -> str:
    return Path(schema_dir).joinpath(
        "%s.schema.json" % compose_full_table_name(project_name, dataset, table_name)
    )


def create_table_if_not_exist(
    project_name: str, dataset_name: str, table_name: str, json_schema
):
    try:
        if not does_bigquery_table_exist(project_name, dataset_name, table_name):
            client = bigquery.Client()
            table_id = compose_full_table_name(project_name, dataset_name, table_name)
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


def compose_full_table_name(project_name: str, dataset_name: str, table_name: str):
    return ".".join([project_name, dataset_name, table_name])


def does_bigquery_table_exist(project_name: str, dataset_name: str, table_name: str):
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = bigquery.Client()
    try:
        client.get_table(table_id)
        return True
    except:
        return False


def get_bigquery_table_schema(project_name: str, dataset_name: str, table_name: str):
    client = bigquery.Client()
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    try:
        table = client.get_table(table_id)
        return [schema_field.to_api_repr() for schema_field in table.schema]
    except:
        return None
