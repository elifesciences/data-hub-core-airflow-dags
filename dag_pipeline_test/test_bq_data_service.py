import pytest
from data_pipeline.utils.cloud_data_store.bq_data_service import  load_file_into_bq
from unittest.mock import patch, mock_open, MagicMock

import data_pipeline.utils.cloud_data_store.bq_data_service as bq_data_service_module



@pytest.fixture(name="bigquery", autouse=True)
def _bigqueryx():
    with patch.object(bq_data_service_module, "bigquery") as mock:
        yield mock

@pytest.fixture(name="bq_client")
def _bigquery():
    with patch.object(bq_data_service_module, "Client") as mock:
        mock.load_table_from_file = bq_data_service_module.Client.load_table_from_file

        yield mock


@pytest.fixture(name="LoadJobConfig")
def _query_job_config():
    with patch.object(bq_data_service_module, "LoadJobConfig") as mock:
        yield mock


@pytest.fixture(name="open_mock", autouse=True)
def _open():
    with patch.object(bq_data_service_module, "open") as mock:
        yield mock


@pytest.fixture(name="mock_isfile")
def _path1():
    with patch.object(bq_data_service_module.os.path, "isfile") as mock:
        mock.return_value = True
        yield mock

@pytest.fixture(name="mock_getsize")
def _path2():
    with patch.object(bq_data_service_module.os.path, "getsize") as mock:
        mock.return_value = 1
        yield mock


class TestBigqueryDataService:

    def test_load_file_into_bq(
            self, LoadJobConfig, open_mock, bq_client, mock_getsize, mock_isfile, bigquery
    ):
        file_name = "file_name"
        dataset_name = "dataset_name"
        table_name = "table_name"
        load_file_into_bq(filename=file_name,dataset_name=dataset_name,table_name=table_name)

        open_mock.assert_called_with(file_name, "rb")
        source_file = open_mock.return_value.__enter__.return_value

        bq_client.assert_called_once()
        bq_client.return_value.dataset.assert_called_with(dataset_name)
        bq_client.return_value.dataset(dataset_name).table.assert_called_with(table_name)

        #bq_client.return_value.load_table_from_file.assert_called_once()
        table_ref = bq_client.return_value.dataset(dataset_name).table(table_name)

        bq_client.return_value.load_table_from_file.assert_called_with(
            source_file, destination=table_ref, job_config=LoadJobConfig.return_value
        )




