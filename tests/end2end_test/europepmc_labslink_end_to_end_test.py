import ftplib
import logging
import gzip
from io import BytesIO

from lxml import etree

from data_pipeline.europepmc.cli_europepmc_labslink import (
    get_europepmc_labslink_pipeline_config,
    main
)

from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    change_or_create_ftp_directory,
    get_connected_ftp_client,
    is_gzip_file_path
)


LOGGER = logging.getLogger(__name__)


def assert_valid_xml_str(xml_str: bytes):
    etree.fromstring(xml_str)


def test_dag_runs_and_uploads_file():
    config = get_europepmc_labslink_pipeline_config()
    ftp_target_config = config.target.ftp
    ftp = get_connected_ftp_client(ftp_target_config)
    change_or_create_ftp_directory(
        ftp,
        directory_name=ftp_target_config.directory_name,
        create_directory=ftp_target_config.create_directory
    )
    filename = ftp_target_config.links_xml_filename
    try:
        LOGGER.info('deleting %r, if it exists', filename)
        ftp.delete(filename)
    except ftplib.error_perm:
        LOGGER.info('failed to delete %r, it may not not exist yet', filename)
    main()
    LOGGER.info('checking ftp file %r', filename)
    data = BytesIO()
    ftp.retrbinary(f'RETR {filename}', callback=data.write)
    xml_str = data.getvalue()
    if is_gzip_file_path(filename):
        LOGGER.info('gzip decompressing file: %d bytes', len(xml_str))
        xml_str = gzip.decompress(xml_str)
    LOGGER.info('checking valid xml: %d bytes', len(xml_str))
    assert_valid_xml_str(xml_str)
    LOGGER.info('done')
