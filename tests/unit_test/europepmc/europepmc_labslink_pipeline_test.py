from unittest.mock import MagicMock, patch

import pytest


from data_pipeline.europepmc.europepmc_labslink_config import (
    BigQuerySourceConfig,
    EuropePmcLabsLinkConfig,
    EuropePmcLabsLinkSourceConfig,
    EuropePmcLabsLinkTargetConfig,
    FtpTargetConfig
)
import data_pipeline.europepmc.europepmc_labslink_pipeline as europepmc_labslink_pipeline_module
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    fetch_article_dois_from_bigquery_and_update_labslink_ftp,
    fetch_article_dois_from_bigquery
)


PROJECT_NAME_1 = 'project1'
SQL_QUERY_1 = 'query1'

BIGQUERY_SOURCE_CONFIG_1 = BigQuerySourceConfig(
    project_name=PROJECT_NAME_1,
    sql_query=SQL_QUERY_1
)

SOURCE_CONFIG_1 = EuropePmcLabsLinkSourceConfig(
    bigquery=BIGQUERY_SOURCE_CONFIG_1
)

FTP_TARGET_CONFIG_1 = FtpTargetConfig(
    host='host1:234',
    username='user1',
    password='password1',
    directory_name='dir1'
)

TARGET_CONFIG_1 = EuropePmcLabsLinkTargetConfig(
    ftp=FTP_TARGET_CONFIG_1
)

CONFIG_1 = EuropePmcLabsLinkConfig(
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)

DOI_LIST = ['doi1', 'doi2']


@pytest.fixture(name='get_single_column_value_list_from_bq_query_mock')
def _get_single_column_value_list_from_bq_query_mock() -> MagicMock:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'get_single_column_value_list_from_bq_query'
    ) as mock:
        yield mock


@pytest.fixture(name='fetch_article_dois_from_bigquery_mock')
def _fetch_article_dois_from_bigquery_mock() -> MagicMock:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'fetch_article_dois_from_bigquery'
    ) as mock:
        yield mock


class TestFetchArticleDoisFromBigQueryAndUpdateLabsLinkFtp:
    def test_should_call_fetch_article_dois_from_bigquery(
        self,
        fetch_article_dois_from_bigquery_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery_and_update_labslink_ftp(
            CONFIG_1
        )
        fetch_article_dois_from_bigquery_mock.assert_called_with(
            BIGQUERY_SOURCE_CONFIG_1
        )


class TestFetchArticleDoisFromBigQuery:
    def test_should_call_get_single_column_value_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery(BIGQUERY_SOURCE_CONFIG_1)
        get_single_column_value_list_from_bq_query_mock.assert_called_with(
            project_name=PROJECT_NAME_1,
            query=SQL_QUERY_1
        )

    def test_should_return_doi_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        get_single_column_value_list_from_bq_query_mock.return_value = DOI_LIST
        actual_doi_list = fetch_article_dois_from_bigquery(BIGQUERY_SOURCE_CONFIG_1)
        assert actual_doi_list == DOI_LIST
