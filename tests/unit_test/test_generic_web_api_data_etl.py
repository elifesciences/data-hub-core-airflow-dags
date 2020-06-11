from datetime import datetime
from unittest.mock import patch
import pytest

from data_pipeline.generic_web_api import (
    generic_web_api_data_etl as generic_web_api_data_etl_module
)
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    upload_latest_timestamp_as_pipeline_state
)
from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.generic_web_api.transform_data import ModuleConstant


@pytest.fixture(name='mock_upload_s3_object')
def _upload_s3_object():
    with patch.object(
            generic_web_api_data_etl_module, 'upload_s3_object'
    ) as mock:
        yield mock


WEB_API_CONFIG = {
    'stateFile': {
        'bucketName': '{ENV}-bucket',
        'objectName': '{ENV}/object'
    },
    'dataUrl': {
        'urlExcludingConfigurableParameters': 'urlExcludingConfigurableParameters'
    }
}
DEP_ENV = 'test'


class TestUploadLatestTimestampState:
    def test_should_write_latest_date_to_state_file(
            self,
            mock_upload_s3_object
    ):
        latest_timestamp_string = '2020-01-01 01:01:01+0100'
        latest_timestamp = datetime.strptime(
            latest_timestamp_string,
            ModuleConstant.DEFAULT_TIMESTAMP_FORMAT
        )
        data_config = WebApiConfig(
            WEB_API_CONFIG, '',
            deployment_env=DEP_ENV
        )
        upload_latest_timestamp_as_pipeline_state(
            data_config, latest_timestamp
        )
        state_file_bucket = WEB_API_CONFIG.get(
            'stateFile'
        ).get('bucketName').replace('{ENV}', DEP_ENV)
        state_file_name_key = WEB_API_CONFIG.get(
            'stateFile'
        ).get('objectName').replace('{ENV}', DEP_ENV)
        mock_upload_s3_object.assert_called_with(
            bucket=state_file_bucket,
            object_key=state_file_name_key,
            data_object=latest_timestamp_string,
        )
