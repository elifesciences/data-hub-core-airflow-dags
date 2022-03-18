from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from lxml import etree

from data_pipeline.europepmc.europepmc_labslink_config import (
    BigQuerySourceConfig,
    EuropePmcLabsLinkConfig,
    EuropePmcLabsLinkSourceConfig,
    EuropePmcLabsLinkTargetConfig,
    EuropePmcLabsLinkXmlConfig,
    FtpTargetConfig
)
import data_pipeline.europepmc.europepmc_labslink_pipeline as europepmc_labslink_pipeline_module
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    fetch_article_dois_from_bigquery_and_update_labslink_ftp,
    fetch_article_dois_from_bigquery,
    generate_labslink_links_xml_to_file_from_doi_list,
    update_labslink_ftp,
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

XML_CONFIG_1 = EuropePmcLabsLinkXmlConfig(
    provider_id='1234',
    link_title='Link Title 1',
    link_prefix='linkPrefix1'
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
    xml=XML_CONFIG_1,
    target=TARGET_CONFIG_1
)

DOI_LIST = ['doi1', 'doi2']


@pytest.fixture(name='ftp_class_mock', autouse=True)
def _ftp_class_mock() -> MagicMock:
    with patch.object(europepmc_labslink_pipeline_module, 'FTP') as mock:
        yield mock


@pytest.fixture(name='ftp_mock')
def _ftp_mock(ftp_class_mock: MagicMock) -> MagicMock:
    return ftp_class_mock.return_value


@pytest.fixture(name='get_single_column_value_list_from_bq_query_mock', autouse=True)
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


@pytest.fixture(name='generate_labslink_links_xml_to_file_from_doi_list_mock')
def _generate_labslink_links_xml_to_file_from_doi_list_mock() -> MagicMock:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'generate_labslink_links_xml_to_file_from_doi_list'
    ) as mock:
        yield mock


@pytest.fixture(name='update_labslink_ftp_mock')
def _update_labslink_ftp_mock() -> MagicMock:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'update_labslink_ftp'
    ) as mock:
        yield mock


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


class TestGenerateLabsLinkLinksXmlToFileFromDoiList:
    def test_should_generate_xml_with_multiple_links(self, tmp_path: Path):
        xml_path = tmp_path / 'links.xml'
        generate_labslink_links_xml_to_file_from_doi_list(
            file_path=str(xml_path),
            doi_list=DOI_LIST,
            xml_config=XML_CONFIG_1
        )
        xml_root = etree.parse(xml_path).getroot()
        assert xml_root.tag == 'links'
        assert xml_root.xpath('link/doi/text()') == DOI_LIST
        assert xml_root.xpath('link/@providerId') == (
            [XML_CONFIG_1.provider_id] * len(DOI_LIST)
        )
        assert xml_root.xpath('link/resource/title/text()') == (
            [XML_CONFIG_1.link_title] * len(DOI_LIST)
        )
        assert xml_root.xpath('link/resource/url/text()') == [
            XML_CONFIG_1.link_prefix + doi
            for doi in DOI_LIST
        ]


class TestUpdateLabsLinkFtp:
    def test_should_upload_links_xml_file(
        self,
        tmp_path: Path,
        ftp_class_mock: MagicMock,
        ftp_mock: MagicMock
    ):
        xml_path = tmp_path / 'links.xml'
        xml_path.write_bytes(b'XML content 1')
        update_labslink_ftp(
            source_xml_file_path=str(xml_path),
            ftp_target_config=FTP_TARGET_CONFIG_1
        )
        ftp_class_mock.assert_called_with(
            host=FTP_TARGET_CONFIG_1.host,
            user=FTP_TARGET_CONFIG_1.username,
            passwd=FTP_TARGET_CONFIG_1.password
        )
        ftp_mock.cwd.assert_called_with(FTP_TARGET_CONFIG_1.directory_name)
        ftp_mock.storbinary.assert_called_with(
            cmd='STOR links.xml',
            fp=ANY
        )


@pytest.mark.usefixtures('update_labslink_ftp_mock')
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

    def test_should_pass_doi_list_to_generate_labslink_links_xml_to_file_from_doi_list(
        self,
        generate_labslink_links_xml_to_file_from_doi_list_mock: MagicMock,
        fetch_article_dois_from_bigquery_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery_mock.return_value = DOI_LIST
        fetch_article_dois_from_bigquery_and_update_labslink_ftp(
            CONFIG_1
        )
        generate_labslink_links_xml_to_file_from_doi_list_mock.assert_called_with(
            file_path=ANY,
            doi_list=DOI_LIST,
            xml_config=XML_CONFIG_1
        )

    def test_should_pass_source_xml_file_path_and_config_to_update_labslink_ftp(
        self,
        update_labslink_ftp_mock: MagicMock,
        generate_labslink_links_xml_to_file_from_doi_list_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery_and_update_labslink_ftp(
            CONFIG_1
        )
        _args, kwargs = generate_labslink_links_xml_to_file_from_doi_list_mock.call_args
        expected_file_path = kwargs['file_path']
        update_labslink_ftp_mock.assert_called_with(
            source_xml_file_path=expected_file_path,
            ftp_target_config=FTP_TARGET_CONFIG_1
        )
