import pytest
from unittest.mock import patch
from math import ceil

from data_pipeline.utils.cloud_data_store.bq_data_service import (
    load_file_into_bq,
    load_tuple_list_into_bq,
)
import data_pipeline.utils.cloud_data_store.bq_data_service as bq_data_service_module


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


class TestBigqueryDataService:
    def test_load_file_into_bq(self, mock_load_job_config, mock_open, mock_bq_client):
        file_name = "file_name"
        dataset_name = "dataset_name"
        table_name = "table_name"
        load_file_into_bq(
            filename=file_name, dataset_name=dataset_name, table_name=table_name
        )

        mock_open.assert_called_with(file_name, "rb")
        source_file = mock_open.return_value.__enter__.return_value

        mock_bq_client.assert_called_once()
        mock_bq_client.return_value.dataset.assert_called_with(dataset_name)
        mock_bq_client.return_value.dataset(dataset_name).table.assert_called_with(
            table_name
        )

        table_ref = mock_bq_client.return_value.dataset(dataset_name).table(table_name)
        mock_bq_client.return_value.load_table_from_file.assert_called_with(
            source_file, destination=table_ref, job_config=mock_load_job_config.return_value
        )

    def test_load_rows_of_tuples_into_bq(self, mock_bq_client):
        number_of_tuples = 10000
        number_of_iteration = ceil(
            number_of_tuples / bq_data_service_module.MAX_ROWS_INSERTABLE
        )
        tuple_list_to_insert = [
            ("test tuple" + str(x), x, False) for x in range(0, number_of_tuples)
        ]

        dataset_name = "dataset_name"
        table_name = "table_name"

        load_tuple_list_into_bq(
            tuple_list_to_insert=tuple_list_to_insert,
            dataset_name=dataset_name,
            table_name=table_name,
        )

        mock_bq_client.assert_called_once()
        mock_bq_client.return_value.dataset.assert_called_with(dataset_name)
        mock_bq_client.return_value.dataset(dataset_name).table.assert_called_with(
            table_name
        )

        assert mock_bq_client.return_value.insert_rows.call_count == number_of_iteration
