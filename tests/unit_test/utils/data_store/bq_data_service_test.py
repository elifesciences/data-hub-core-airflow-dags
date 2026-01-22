from math import ceil
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from google.cloud.bigquery.table import Row

import data_pipeline.utils.data_store.bq_data_service \
    as bq_data_service_module
from data_pipeline.utils.data_store.bq_data_service import (
    get_distinct_values_from_bq,
    get_max_value_from_bq_table,
    get_query_with_exclusion,
    iter_dict_from_bq_query,
    load_file_into_bq,
    load_tuple_list_into_bq,
    get_new_merged_schema
)


@pytest.fixture(name="mock_bigquery")
def _bigquery():
    with patch.object(bq_data_service_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="bq_client_class_mock")
def _bq_client_class_mock():
    with patch.object(bq_data_service_module, "Client") as mock:
        yield mock


@pytest.fixture(name="bq_client_mock")
def _bq_client_mock(bq_client_class_mock: MagicMock):
    yield bq_client_class_mock.return_value


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


class TestGetQueryWithExclusion:
    def test_should_return_regular_query_without_exclusion(self):
        assert get_query_with_exclusion(
            'query1',
            key_field_name='key1'
        ) == 'query1'

    def test_should_wrap_query_and_add_where_clause(self):
        assert get_query_with_exclusion(
            'SELECT "key1" AS key',
            key_field_name='key',
            exclude_query='SELECT "key1" AS key'
        ) == '\n'.join([
            'SELECT * FROM (',
            'SELECT "key1" AS key',
            ')',
            'WHERE key NOT IN (',
            'SELECT "key1" AS key',
            ')'
        ])


class TestIterDictFromBqQuery:
    def test_should_return_dict_for_row(self, bq_client_class_mock: MagicMock):
        mock_query_job = bq_client_class_mock.return_value.query.return_value
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


class TestGetDistinctValuesFromBq:
    def test_should_generate_query_for_minimal_parameters(
        self,
        bq_client_mock: MagicMock
    ):
        get_distinct_values_from_bq(
            project_name="project_1",
            dataset_name="dataset_1",
            column_name="column_name_1",
            table_name_source="table_name_1"
        )
        query_mock: MagicMock = bq_client_mock.query
        args, _kwargs = query_mock.call_args
        assert textwrap.dedent(args[0]).strip() == textwrap.dedent(
            '''
            SELECT DISTINCT column_name_1 AS column
            FROM  `project_1.dataset_1.table_name_1`
            WHERE column_name_1 IS NOT NULL
            '''
        ).strip()

    def test_should_generate_query_with_table_name_for_exclusion_param(
        self,
        bq_client_mock: MagicMock
    ):
        get_distinct_values_from_bq(
            project_name="project_1",
            dataset_name="dataset_1",
            column_name="column_name_1",
            table_name_source="table_name_1",
            table_name_for_exclusion="table_name_for_exclusion_1"
        )
        query_mock: MagicMock = bq_client_mock.query
        args, _kwargs = query_mock.call_args
        assert textwrap.dedent(args[0]).strip() == textwrap.dedent(
            '''
            SELECT DISTINCT column_name_1 AS column
            FROM  `project_1.dataset_1.table_name_1`
            WHERE column_name_1 IS NOT NULL

            AND column_name_1 NOT IN
                (
                    SELECT column_name_1
                    FROM `project_1.dataset_1.table_name_for_exclusion_1`
                )
            '''
        ).strip()

    def test_should_generate_query_with_array_table_name_param(
        self,
        bq_client_mock: MagicMock
    ):
        get_distinct_values_from_bq(
            project_name="project_1",
            dataset_name="dataset_1",
            column_name="column_name_1",
            table_name_source="table_name_1",
            table_name_for_exclusion="table_name_for_exclusion_1",
            array_table_name="array_table_name_1",
            array_column_for_exclusion="array_column_for_exclusion_1"
        )
        query_mock: MagicMock = bq_client_mock.query
        args, _kwargs = query_mock.call_args
        assert textwrap.dedent(args[0]).strip() == textwrap.dedent(
            '''
            SELECT DISTINCT column_name_1 AS column
            FROM  `project_1.dataset_1.table_name_1`
            WHERE column_name_1 IS NOT NULL

            AND array_column_for_exclusion_1 NOT IN
                (
                    SELECT t_array.array_column_for_exclusion_1
                    FROM `project_1.dataset_1.table_name_for_exclusion_1`
                    LEFT JOIN UNNEST(array_table_name_1) AS t_array
                )
            '''
        ).strip()


class TestGetMaxValueFromBqTable:
    def test_should_generate_query_for_max_value(
        self,
        bq_client_mock: MagicMock
    ):
        query_mock: MagicMock = bq_client_mock.query
        query_job_mock = query_mock.return_value
        query_job_mock.result.return_value = [
            Row(["max_value_1"], {"max_value": 0})
        ]
        get_max_value_from_bq_table(
            project_name="project_1",
            dataset_name="dataset_1",
            column_name="column_name_1",
            table_name="table_name_1"
        )
        args, _kwargs = query_mock.call_args
        assert textwrap.dedent(args[0]).strip() == textwrap.dedent(
            '''
            SELECT
            MAX(column_name_1) AS max_value
            FROM `project_1.dataset_1.table_name_1`
            '''
        ).strip()


def test_should_load_file_into_bq(
        mock_load_job_config,
        mock_open,
        bq_client_class_mock):

    file_name = "file_name"
    project_name = "project_name"
    dataset_name = "dataset_name"
    table_name = "table_name"
    load_file_into_bq(
        filename=file_name,
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=table_name
    )

    mock_open.assert_called_with(file_name, "rb")
    source_file = mock_open.return_value.__enter__.return_value

    bq_client_class_mock.assert_called_once()
    bq_client_class_mock.return_value.dataset.assert_called_with(dataset_name)
    bq_client_class_mock.return_value.dataset(
        dataset_name).table.assert_called_with(table_name)

    table_ref = bq_client_class_mock.return_value.dataset(
        dataset_name).table(table_name)
    bq_client_class_mock.return_value.load_table_from_file.assert_called_with(
        source_file, destination=table_ref,
        job_config=mock_load_job_config.return_value)


def test_should_load_rows_of_tuples_into_bq(bq_client_class_mock):
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

    bq_client_class_mock.assert_called_once()
    bq_client_class_mock.return_value.dataset.assert_called_with(dataset_name)
    bq_client_class_mock.return_value.dataset(
        dataset_name).table.assert_called_with(table_name)


def test_count_of_iteration_when_loading_list_of_rows_into_bq(bq_client_class_mock):
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
    assert bq_client_class_mock.return_value.insert_rows.call_count == \
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
