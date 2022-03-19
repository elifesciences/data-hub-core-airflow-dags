import ftplib
import logging
from io import BytesIO

from lxml import etree

from dags.europepmc_labslink_data_export_pipeline import (
    DAG_ID,
    get_pipeline_config
)
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    change_or_create_ftp_directory,
    get_connected_ftp_client
)

from tests.end2end_test import (
    enable_and_trigger_dag_and_wait_for_success
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


LOGGER = logging.getLogger(__name__)


def assert_valid_xml_str(xml_str: bytes):
    etree.fromstring(xml_str)


def test_dag_runs_and_uploads_file():
    airflow_api = AirflowAPI()
    config = get_pipeline_config()
    ftp_target_config = config.target.ftp
    ftp = get_connected_ftp_client(ftp_target_config)
    change_or_create_ftp_directory(
        ftp,
        directory_name=ftp_target_config.directory_name,
        create_directory=ftp_target_config.create_directory
    )
    filename = 'links.xml'
    try:
        LOGGER.info('deleting %r, if it exists', filename)
        ftp.delete('links.xml')
    except ftplib.error_perm:
        LOGGER.info('failed to delete %r, it may not not exist yet', filename)
    enable_and_trigger_dag_and_wait_for_success(
        airflow_api=airflow_api,
        dag_id=DAG_ID
    )
    LOGGER.info('checking ftp file %r', filename)
    data = BytesIO()
    ftp.retrbinary('RETR %s' % filename, callback=data.write)
    xml_str = data.getvalue()
    LOGGER.info('checking valid xml: %d bytes', len(xml_str))
    assert_valid_xml_str(xml_str)
    LOGGER.info('done')
