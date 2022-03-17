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
    fetch_article_dois_from_bigquery_and_update_labslink_ftp
)


BIGQUERY_SOURCE_CONFIG_1 = BigQuerySourceConfig(
    project_name='project1',
    sql_query='query1'
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
