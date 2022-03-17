import logging
import os
from tempfile import TemporaryDirectory
from typing import Sequence

from lxml import etree
from lxml.builder import E

from data_pipeline.europepmc.europepmc_labslink_config import (
    BigQuerySourceConfig,
    EuropePmcLabsLinkConfig,
    FtpTargetConfig
)
from data_pipeline.utils.data_store.bq_data_service import (
    get_single_column_value_list_from_bq_query
)


LOGGER = logging.getLogger(__name__)


class LabsLinkElementMakers:
    LINKS = E.links
    LINK = E.link
    DOI = E.doi


def fetch_article_dois_from_bigquery(
    bigquery_source_config: BigQuerySourceConfig
) -> Sequence[str]:
    LOGGER.debug('bigquery_source: %r', bigquery_source_config)
    doi_list = get_single_column_value_list_from_bq_query(
        project_name=bigquery_source_config.project_name,
        query=bigquery_source_config.sql_query
    )
    LOGGER.debug('doi_list: %r', doi_list)
    LOGGER.info('length of doi_list: %r', len(doi_list))
    return doi_list


def create_labslink_link_xml_node_for_doi(doi: str) -> etree.ElementBase:
    return LabsLinkElementMakers.LINK(
        LabsLinkElementMakers.DOI(doi)
    )


def generate_labslink_links_xml_to_file_from_doi_list(
    file_path: str,
    doi_list: Sequence[str]
):
    LOGGER.info("file_path: %r", file_path)
    LOGGER.debug("doi_list: %r", doi_list)
    xml_root = LabsLinkElementMakers.LINKS(*[
        create_labslink_link_xml_node_for_doi(doi)
        for doi in doi_list
    ])
    with open(file_path, 'wb') as xml_fp:
        xml_fp.write(etree.tostring(xml_root))


def update_labslink_ftp(
    source_xml_file_path: str,
    ftp_target_config: FtpTargetConfig
):
    LOGGER.info("source_xml_file_path: %r", source_xml_file_path)
    LOGGER.debug("ftp_target_config: %r", ftp_target_config)


def fetch_article_dois_from_bigquery_and_update_labslink_ftp(
    config: EuropePmcLabsLinkConfig
):
    LOGGER.debug('config: %r', config)
    article_dois = fetch_article_dois_from_bigquery(config.source.bigquery)
    LOGGER.debug('article_dois: %r', article_dois)

    with TemporaryDirectory() as tmp_dir:
        temp_file_path = os.path.join(tmp_dir, 'links.xml')

        generate_labslink_links_xml_to_file_from_doi_list(
            file_path=temp_file_path,
            doi_list=article_dois
        )

        update_labslink_ftp(
            source_xml_file_path=temp_file_path,
            ftp_target_config=config.target.ftp
        )
