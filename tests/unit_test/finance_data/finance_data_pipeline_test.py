from unittest.mock import MagicMock, patch
import pytest
from google.cloud import bigquery

import data_pipeline.finance_data.finance_data_pipeline as finance_data_pipeline_module

from data_pipeline.finance_data.finance_data_pipeline import (
    read_finance_data_from_bigquery,
    fetch_finance_data_from_bigquery_and_write_to_s3,
    fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list
)
from data_pipeline.finance_data.finance_data_pipeline_config import (
    FinanceDataSourceConfig,
    FinanceDataTargetConfig,
    FinanceDataPipelineConfig
)


@pytest.fixture(name='bigquery_client_mock', autouse=True)
def _bigquery_client_mock():
    with patch.object(bigquery, 'Client') as mock:
        yield mock


@pytest.fixture(name='write_dataframe_to_s3_bucket_mock', autouse=True)
def _write_dataframe_to_s3_bucket_mock():
    with patch.object(finance_data_pipeline_module, 'write_dataframe_to_s3_bucket') as mock:
        yield mock


@pytest.fixture(name='read_finance_data_from_bigquery_mock', autouse=True)
def _read_finance_data_from_bigquery_mock():
    with patch.object(finance_data_pipeline_module, 'read_finance_data_from_bigquery') as mock:
        yield mock


PROJECT_NAME = 'project_1'
DATASET = 'dataset_1'
TABLE = 'table_1'
BUCKET = 'bucket_1'
OBJECT_NAME = 'object_1.csv'
DATA_PIPELINE_ID_1 = 'pipeline_1'
DATA_PIPELINE_ID_2 = 'pipeline_2'

SOURCE_CONFIG_1 = FinanceDataSourceConfig(
    project_name=PROJECT_NAME,
    dataset=DATASET,
    table=TABLE
)

TARGET_CONFIG_1 = FinanceDataTargetConfig(
    bucket=BUCKET,
    object_name=OBJECT_NAME
)

PIPELINE_CONFIG_1 = FinanceDataPipelineConfig(
    data_pipeline_id=DATA_PIPELINE_ID_1,
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)

PIPELINE_CONFIG_2 = FinanceDataPipelineConfig(
    data_pipeline_id=DATA_PIPELINE_ID_2,
    source=FinanceDataSourceConfig(
        project_name='project_2',
        dataset='dataset_2',
        table='table_2'
    ),
    target=FinanceDataTargetConfig(
        bucket='bucket_2',
        object_name='object_2.csv'
    )
)


class TestReadFinanceDataFromBigQuery:
    def test_should_execute_query_with_correct_parameters(
        self,
        bigquery_client_mock: MagicMock
    ):
        mock_client_instance = bigquery_client_mock.return_value
        mock_query_job = mock_client_instance.query.return_value
        mock_query_job.to_dataframe.return_value = MagicMock()

        read_finance_data_from_bigquery(
            project_name=PROJECT_NAME,
            dataset=DATASET,
            table=TABLE
        )

        mock_client_instance.query.assert_called_once_with(
            f'SELECT * FROM {PROJECT_NAME}.{DATASET}.{TABLE}'
        )


class TestFetchFinanceDataFromBigQueryAndWriteToS3:
    def test_should_read_data_and_write_to_s3(
        self,
        read_finance_data_from_bigquery_mock: MagicMock,
        write_dataframe_to_s3_bucket_mock: MagicMock
    ):
        read_finance_data_from_bigquery_mock.return_value = MagicMock()

        fetch_finance_data_from_bigquery_and_write_to_s3(
            source_config=SOURCE_CONFIG_1,
            target_config=TARGET_CONFIG_1
        )

        read_finance_data_from_bigquery_mock.assert_called_once_with(
            project_name=PROJECT_NAME,
            dataset=DATASET,
            table=TABLE
        )
        write_dataframe_to_s3_bucket_mock.assert_called_once_with(
            df_name=read_finance_data_from_bigquery_mock.return_value,
            bucket=BUCKET,
            object_name=OBJECT_NAME
        )


class TestFetchFinanceDataFromBigQueryAndWriteToS3FromConfigList:
    def test_should_process_all_pipeline_configs(
        self,
        read_finance_data_from_bigquery_mock: MagicMock,
        write_dataframe_to_s3_bucket_mock: MagicMock
    ):
        configs = [PIPELINE_CONFIG_1, PIPELINE_CONFIG_2]
        read_finance_data_from_bigquery_mock.return_value = MagicMock()

        fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list(configs)

        assert read_finance_data_from_bigquery_mock.call_count == 2
        assert write_dataframe_to_s3_bucket_mock.call_count == 2
        write_dataframe_to_s3_bucket_mock.assert_any_call(
            df_name=read_finance_data_from_bigquery_mock.return_value,
            bucket=BUCKET,
            object_name=OBJECT_NAME
        )
        write_dataframe_to_s3_bucket_mock.assert_any_call(
            df_name=read_finance_data_from_bigquery_mock.return_value,
            bucket='bucket_2',
            object_name='object_2.csv'
        )
