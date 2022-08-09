from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest

from data_pipeline.twitter_ads_api import (
    twitter_ads_api_pipeline as twitter_ads_api_pipeline_module
)

from data_pipeline.twitter_ads_api.twitter_ads_api_pipeline import (
    get_bq_compatible_json_response_from_resource_with_provenance,
    iter_bq_compatible_json_response_from_resource_with_provenance
)

from data_pipeline.utils.pipeline_config import (
    BigQueryTargetConfig
)

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiSourceConfig
)

RESOURCE = 'resource_1'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SOURCE_CONFIG_1 = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS
)

PARAMS = {'param1': 'value1'}

TARGET_CONFIG_1 = BigQueryTargetConfig(
    project_name='project1',
    dataset_name='dataset1',
    table_name='table1'
)

RESPONSE_JSON_1 = {
    'response_key_1': 'response_value_1',
    'response_key_2': 'response_value_2'
}

MOCK_UTC_NOW_STR = '2022-08-03T16:35:56'

PROVENANCE_1 = {
    'imported_timestamp': MOCK_UTC_NOW_STR,
    'request_resource': RESOURCE
}


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


@pytest.fixture(name='get_client_from_twitter_ads_api_mock')
def _get_client_from_twitter_ads_api_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'get_client_from_twitter_ads_api') as mock:
        yield mock


@pytest.fixture(name='remove_key_with_null_value_mock')
def _remove_key_with_null_value_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'remove_key_with_null_value') as mock:
        yield mock


@pytest.fixture(name='request_mock', autouse=True)
def _request_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'Request') as mock:
        yield mock


@pytest.fixture(
    name='get_bq_compatible_json_response_from_resource_with_provenance_mock',
    autouse=True
)
def _get_bq_compatible_json_response_from_resource_with_provenance_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'get_bq_compatible_json_response_from_resource_with_provenance'
    ) as mock:
        yield mock


@pytest.fixture(
    name='fetch_single_column_value_list_for_bigquery_source_config_mock',
    autouse=True
)
def _fetch_single_column_value_list_for_bigquery_source_config_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'fetch_single_column_value_list_for_bigquery_source_config'
    ) as mock:
        yield mock


class TestGetBqCompatibleJsonResponseRromResourceWithProvenance:
    def test_should_pass_resource_to_request(
        self,
        request_mock: MagicMock,
        get_client_from_twitter_ads_api_mock: MagicMock
    ):
        get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1
        )
        request_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource=RESOURCE,
            params=None
        )

    def test_should_pass_params_to_request_if_params_defined(
        self,
        request_mock: MagicMock,
        get_client_from_twitter_ads_api_mock: MagicMock
    ):
        get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1,
            PARAMS
        )
        request_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource=RESOURCE,
            params=PARAMS
        )

    def test_should_return_response_json_with_provenance(
        self,
        get_client_from_twitter_ads_api_mock: MagicMock,
        remove_key_with_null_value_mock: MagicMock
    ):
        get_client_from_twitter_ads_api_mock.return_value = 'client'
        remove_key_with_null_value_mock.return_value = RESPONSE_JSON_1
        actual_response_json = get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1
        )
        assert actual_response_json == {**RESPONSE_JSON_1, 'provenance': PROVENANCE_1}

    def test_should_return_response_json_with_provenance_include_params_if_defined(
        self,
        get_client_from_twitter_ads_api_mock: MagicMock,
        remove_key_with_null_value_mock: MagicMock
    ):
        get_client_from_twitter_ads_api_mock.return_value = 'client'
        remove_key_with_null_value_mock.return_value = RESPONSE_JSON_1
        actual_response_json = get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1,
            PARAMS
        )
        assert actual_response_json == {
            **RESPONSE_JSON_1,
            'provenance': {
                **PROVENANCE_1,
                'request_params': [{'name': 'param1', 'value': PARAMS['param1']}]
            }
        }


class TestIterBqCompatibleJsonResponseFromResourceWithProvenance:
    def test_should_pass_none_to_params_dict_if_param_names_and_bigquery_not_exist_in_source_conf(
        self,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock
    ):
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_1,
            params_dict=None
        )
