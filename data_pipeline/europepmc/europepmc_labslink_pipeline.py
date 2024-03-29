import ftplib
import logging
import gzip
import os
from ftplib import FTP
from tempfile import TemporaryDirectory
from typing import Sequence

from lxml import etree
from lxml.builder import E

from data_pipeline.utils.pipeline_utils import (
    fetch_single_column_value_list_for_bigquery_source_config
)
from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig,
    EuropePmcLabsLinkXmlConfig,
    FtpTargetConfig
)


LOGGER = logging.getLogger(__name__)


class LabsLinkElementMakers:
    LINKS = E.links
    LINK = E.link
    DOI = E.doi
    RESOURCE = E.resource
    TITLE = E.title
    URL = E.url


def create_labslink_link_xml_node_for_doi(
    doi: str,
    xml_config: EuropePmcLabsLinkXmlConfig
) -> etree.ElementBase:
    return LabsLinkElementMakers.LINK(
        {'providerId': xml_config.provider_id},
        LabsLinkElementMakers.RESOURCE(
            LabsLinkElementMakers.TITLE(xml_config.link_title),
            LabsLinkElementMakers.URL(xml_config.link_prefix + doi)
        ),
        LabsLinkElementMakers.DOI(doi)
    )


def is_gzip_file_path(file_path: str) -> bool:
    return file_path.endswith('.gz')


def generate_labslink_links_xml_to_file_from_doi_list(
    file_path: str,
    doi_list: Sequence[str],
    xml_config: EuropePmcLabsLinkXmlConfig
):
    LOGGER.info("file_path: %r", file_path)
    LOGGER.debug("doi_list: %r", doi_list)
    LOGGER.info('xml_config: %r', xml_config)
    xml_root = LabsLinkElementMakers.LINKS(*[
        create_labslink_link_xml_node_for_doi(doi, xml_config=xml_config)
        for doi in doi_list
    ])
    with open(file_path, 'wb') as xml_fp:
        data = etree.tostring(
            xml_root,
            pretty_print=True,
            xml_declaration=True,
            encoding='UTF-8',
            standalone=True
        )
        if is_gzip_file_path(file_path):
            data = gzip.compress(data)
        xml_fp.write(data)


def get_connected_ftp_client(
    ftp_target_config: FtpTargetConfig
) -> FTP:
    LOGGER.info('creating FTP connection')
    ftp = FTP()
    ftp.connect(
        host=ftp_target_config.hostname,
        port=ftp_target_config.port
    )
    ftp.login(
        user=ftp_target_config.username,
        passwd=ftp_target_config.password
    )
    return ftp


def change_or_create_ftp_directory(
    ftp: FTP,
    directory_name: str,
    create_directory: bool
):
    LOGGER.info('changing directory')
    try:
        ftp.cwd(directory_name)
    except ftplib.error_perm:
        if not create_directory:
            raise
        LOGGER.info("failed to change directory, attempting to create it")
        ftp.mkd(directory_name)
        ftp.cwd(directory_name)


def update_labslink_ftp(
    source_xml_file_path: str,
    ftp_target_config: FtpTargetConfig
):
    LOGGER.info("source_xml_file_path: %r", source_xml_file_path)
    LOGGER.debug("ftp_target_config: %r", ftp_target_config)
    LOGGER.info('creating FTP connection')
    ftp = get_connected_ftp_client(ftp_target_config)
    change_or_create_ftp_directory(
        ftp,
        directory_name=ftp_target_config.directory_name,
        create_directory=ftp_target_config.create_directory
    )
    LOGGER.info('uploading file')
    with open(source_xml_file_path, 'rb') as xml_fp:
        ftp.storbinary(cmd=f'STOR {ftp_target_config.links_xml_filename}', fp=xml_fp)


def fetch_article_dois_from_bigquery_and_update_labslink_ftp(
    config: EuropePmcLabsLinkConfig
):
    LOGGER.debug('config: %r', config)
    article_dois = fetch_single_column_value_list_for_bigquery_source_config(
        config.source.bigquery
    )
    LOGGER.debug('article_dois: %r', article_dois)

    with TemporaryDirectory() as tmp_dir:
        temp_file_path = os.path.join(tmp_dir, config.target.ftp.links_xml_filename)

        generate_labslink_links_xml_to_file_from_doi_list(
            file_path=temp_file_path,
            doi_list=article_dois,
            xml_config=config.xml
        )

        update_labslink_ftp(
            source_xml_file_path=temp_file_path,
            ftp_target_config=config.target.ftp
        )
