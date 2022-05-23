from math import ceil
from unittest.mock import MagicMock, patch

import pytest

from google.cloud.bigquery.table import Row

import data_pipeline.utils.data_store.bq_data_service \
    as bq_data_service_module
from data_pipeline.utils.data_store.bq_data_service import (
    iter_dict_from_bq_query,
    load_file_into_bq,
    load_tuple_list_into_bq,
    get_new_merged_schema
)


@pytest.fixture(name="mock_bigquery")
def _bigquery():
    with patch.object(bq_data_service_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="mock_bq_client")
def _bq_client():
    with patch.object(bq_data_service_module, "Client") as mock:
        yield mock


@pytest.fixture(name="mock_load_job_config")
def _load_job_config():
    with patch.object(bq_data_service_module, "LoadJobConfig") as mock:
        yield mock


@pytest.fixture(name="mock_open", autouse=True)
def _open():
    with patch.object(bq_data_service_module, "open") as mock:
        yield mock


@pytest.fixture(name="mock_path")
def _getsize():
    with patch.object(bq_data_service_module.os, "path") as mock:
        mock.return_value.getsize = 1
        mock.return_value.isfile = True
        yield mock


class TestIterDictFromBqQuery:
    def test_should_return_dict_for_row(self, mock_bq_client: MagicMock):
        mock_query_job = mock_bq_client.return_value.query.return_value
        mock_query_job.result.return_value = [
            Row(["value1", "value2"], {"key1": 0, "key2": 1})
        ]
        result = list(iter_dict_from_bq_query(
            project_name="project1",
            query="query1"
        ))
        assert result == [{
            "key1": "value1",
            "key2": "value2"
        }]


def test_should_load_file_into_bq(
        mock_load_job_config,
        mock_open,
        mock_bq_client):

    file_name = "file_name"
    dataset_name = "dataset_name"
    table_name = "table_name"
    load_file_into_bq(
        filename=file_name,
        dataset_name=dataset_name,
        table_name=table_name
    )

    mock_open.assert_called_with(file_name, "rb")
    source_file = mock_open.return_value.__enter__.return_value

    mock_bq_client.assert_called_once()
    mock_bq_client.return_value.dataset.assert_called_with(dataset_name)
    mock_bq_client.return_value.dataset(
        dataset_name).table.assert_called_with(table_name)

    table_ref = mock_bq_client.return_value.dataset(
        dataset_name).table(table_name)
    mock_bq_client.return_value.load_table_from_file.assert_called_with(
        source_file, destination=table_ref,
        job_config=mock_load_job_config.return_value)


def test_should_load_rows_of_tuples_into_bq(mock_bq_client):
    number_of_tuples = 10000
    tuple_list_to_insert = [
        ("test tuple" + str(x), x, False)
        for x in range(0, number_of_tuples)]

    dataset_name = "dataset_name"
    table_name = "table_name"

    load_tuple_list_into_bq(
        tuple_list_to_insert=tuple_list_to_insert,
        dataset_name=dataset_name,
        table_name=table_name,
    )

    mock_bq_client.assert_called_once()
    mock_bq_client.return_value.dataset.assert_called_with(dataset_name)
    mock_bq_client.return_value.dataset(
        dataset_name).table.assert_called_with(table_name)


def test_count_of_iteration_when_loading_list_of_rows_into_bq(mock_bq_client):
    number_of_tuples = 10000
    number_of_iteration = ceil(
        number_of_tuples / bq_data_service_module.MAX_ROWS_INSERTABLE
    )
    tuple_list_to_insert = [
        ("test tuple" + str(x), x, False)
        for x in range(0, number_of_tuples)]

    dataset_name = "dataset_name"
    table_name = "table_name"

    load_tuple_list_into_bq(
        tuple_list_to_insert=tuple_list_to_insert,
        dataset_name=dataset_name,
        table_name=table_name,
    )
    assert mock_bq_client.return_value.insert_rows.call_count == \
        number_of_iteration


def test_should_merge_top_level_and_nested_fields():
    existing_schema = [
        {"name": "imported_timestamp", "type": "TIMESTAMP"},
        {"name": "univ", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"type": "RECORD", "name": "provenance",
         "fields":
             [
                 {"name": "s3_bucket", "type": "STRING"},
                 {"name": "source_filename", "type": "STRING"}
             ]
         }
    ]
    new_schema = [
        {"name": "country", "type": "STRING"},
        {"name": "new_field", "type": "STRING"},
        {"type": "RECORD", "name": "provenance",
         "fields":
             [
                 {"name": "new_s3_bucket", "type": "STRING"},
                 {"name": "source_filename", "type": "STRING"}
             ]
         }
    ]
    computed_schema = get_new_merged_schema(
        existing_schema,
        new_schema
    )

    expected_schema = [
        {"name": "country", "type": "STRING"},
        {"name": "new_field", "type": "STRING"},
        {"name": "imported_timestamp", "type": "TIMESTAMP"},
        {"name": "univ", "type": "STRING"},
        {"type": "RECORD", "name": "provenance",
         "fields":
             [
                 {"name": "new_s3_bucket", "type": "STRING"},
                 {"name": "source_filename", "type": "STRING"},
                 {"name": "s3_bucket", "type": "STRING"}
             ]
         }
    ]
    assert computed_schema == expected_schema


def test_should_not_update_existing_fields():
    existing_schema = [
        {"name": "imported_timestamp", "type": "TIMESTAMP"},
        {"name": "univ", "type": "STRING"},
        {"name": "country", "type": "STRING"}
    ]
    new_schema = [
        {"name": "country", "type": "INT"},
    ]
    computed_schema = get_new_merged_schema(
        existing_schema,
        new_schema
    )
    expected_schema = [
        {'name': 'country', 'type': 'STRING'},
        {'name': 'imported_timestamp', 'type': 'TIMESTAMP'},
        {'name': 'univ', 'type': 'STRING'}
    ]
    assert computed_schema == expected_schema
