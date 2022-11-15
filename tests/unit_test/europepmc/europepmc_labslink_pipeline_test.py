import ftplib
import logging
import gzip
from pathlib import Path
from typing import Iterable
from unittest.mock import ANY, MagicMock, patch

import pytest

from lxml import etree

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig,
    EuropePmcLabsLinkSourceConfig,
    EuropePmcLabsLinkTargetConfig,
    EuropePmcLabsLinkXmlConfig,
    FtpTargetConfig
)
import data_pipeline.europepmc.europepmc_labslink_pipeline as europepmc_labslink_pipeline_module
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    fetch_article_dois_from_bigquery_and_update_labslink_ftp,
    generate_labslink_links_xml_to_file_from_doi_list,
    update_labslink_ftp,
)


LOGGER = logging.getLogger(__name__)


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
    hostname='host1',
    port=123,
    username='user1',
    password='password1',
    directory_name='dir1',
    links_xml_filename='links1.xml'
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
def _ftp_class_mock() -> Iterable[MagicMock]:
    with patch.object(europepmc_labslink_pipeline_module, 'FTP') as mock:
        yield mock


@pytest.fixture(name='ftp_mock')
def _ftp_mock(ftp_class_mock: MagicMock) -> MagicMock:
    return ftp_class_mock.return_value


@pytest.fixture(name='fetch_single_column_value_list_for_bigquery_source_config_mock', autouse=True)
def _fetch_single_column_value_list_for_bigquery_source_config_mock() -> Iterable[MagicMock]:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'fetch_single_column_value_list_for_bigquery_source_config'
    ) as mock:
        yield mock


@pytest.fixture(name='generate_labslink_links_xml_to_file_from_doi_list_mock')
def _generate_labslink_links_xml_to_file_from_doi_list_mock() -> Iterable[MagicMock]:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'generate_labslink_links_xml_to_file_from_doi_list'
    ) as mock:
        yield mock


@pytest.fixture(name='update_labslink_ftp_mock')
def _update_labslink_ftp_mock() -> Iterable[MagicMock]:
    with patch.object(
        europepmc_labslink_pipeline_module,
        'update_labslink_ftp'
    ) as mock:
        yield mock


def _check_valid_xml_str(xml_str: bytes):
    etree.fromstring(xml_str)


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

    def test_should_have_xml_declaration(self, tmp_path: Path):
        xml_path = tmp_path / 'links.xml'
        generate_labslink_links_xml_to_file_from_doi_list(
            file_path=str(xml_path),
            doi_list=DOI_LIST,
            xml_config=XML_CONFIG_1
        )
        xml_str = xml_path.read_text()
        LOGGER.debug('xml_str: %r', xml_str)
        first_line = xml_str.splitlines()[0]
        assert first_line.startswith('<?xml')
        assert "encoding='UTF-8'" in first_line
        assert "standalone='yes'" in first_line

    def test_should_gzip_compress_for_filenames_with_gz_extension(self, tmp_path: Path):
        xml_path = tmp_path / 'links.xml.gz'
        generate_labslink_links_xml_to_file_from_doi_list(
            file_path=str(xml_path),
            doi_list=DOI_LIST,
            xml_config=XML_CONFIG_1
        )
        _check_valid_xml_str(gzip.decompress(xml_path.read_bytes()))


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
        ftp_class_mock.assert_called()
        ftp_mock.connect.assert_called_with(
            host=FTP_TARGET_CONFIG_1.hostname,
            port=FTP_TARGET_CONFIG_1.port
        )
        ftp_mock.login.assert_called_with(
            user=FTP_TARGET_CONFIG_1.username,
            passwd=FTP_TARGET_CONFIG_1.password
        )
        ftp_mock.cwd.assert_called_with(FTP_TARGET_CONFIG_1.directory_name)
        ftp_mock.storbinary.assert_called_with(
            cmd=f'STOR {FTP_TARGET_CONFIG_1.links_xml_filename}',
            fp=ANY
        )

    def test_should_not_create_directory_when_permission_error_raised_if_not_enabled(
        self,
        tmp_path: Path,
        ftp_mock: MagicMock
    ):
        xml_path = tmp_path / 'links.xml'
        xml_path.write_bytes(b'XML content 1')
        ftp_mock.cwd.side_effect = ftplib.error_perm()
        with pytest.raises(ftplib.error_perm):
            update_labslink_ftp(
                source_xml_file_path=str(xml_path),
                ftp_target_config=FTP_TARGET_CONFIG_1._replace(
                    create_directory=False
                )
            )
        ftp_mock.mkd.assert_not_called()

    def test_should_create_directory_when_permission_error_raised_if_enabled(
        self,
        tmp_path: Path,
        ftp_mock: MagicMock
    ):
        xml_path = tmp_path / 'links.xml'
        xml_path.write_bytes(b'XML content 1')
        ftp_mock.cwd.side_effect = [
            ftplib.error_perm(),  # first cwd call should fail
            None  # second cwd call will succeed (no return value)
        ]
        update_labslink_ftp(
            source_xml_file_path=str(xml_path),
            ftp_target_config=FTP_TARGET_CONFIG_1._replace(
                create_directory=True
            )
        )
        ftp_mock.mkd.assert_called_with(FTP_TARGET_CONFIG_1.directory_name)
        assert ftp_mock.cwd.call_count == 2


@pytest.mark.usefixtures('update_labslink_ftp_mock')
class TestFetchArticleDoisFromBigQueryAndUpdateLabsLinkFtp:
    def test_should_call_fetch_article_dois_from_bigquery(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery_and_update_labslink_ftp(
            CONFIG_1
        )
        fetch_single_column_value_list_for_bigquery_source_config_mock.assert_called_with(
            BIGQUERY_SOURCE_CONFIG_1
        )

    def test_should_pass_doi_list_to_generate_labslink_links_xml_to_file_from_doi_list(
        self,
        generate_labslink_links_xml_to_file_from_doi_list_mock: MagicMock,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = DOI_LIST
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

    def test_should_pass_configured_links_xml_filename_to_generate_function(
        self,
        generate_labslink_links_xml_to_file_from_doi_list_mock: MagicMock
    ):
        fetch_article_dois_from_bigquery_and_update_labslink_ftp(
            CONFIG_1
        )
        _args, kwargs = generate_labslink_links_xml_to_file_from_doi_list_mock.call_args
        links_xml_file_path = kwargs['file_path']
        assert links_xml_file_path.endswith(CONFIG_1.target.ftp.links_xml_filename)
