from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock
import pytest
import logging

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

LOGGER = logging.getLogger(__name__)

RESOURCE = 'resource_1'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SOURCE_CONFIG_1 = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS
)
PARAMS = {'param1': 'value1'}

PARAM_NAMES = ['param_name_1', 'param_name_2', 'param_name_3']
PARAM_VALUES_FROM_BQ = ['bq_param_1']

SOURCE_CONFIG_WITH_PARAM_NAMES_AND_BQ = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS,
    param_names=PARAM_NAMES,
    param_from_bigquery=PARAM_VALUES_FROM_BQ
)

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


@pytest.fixture(name='request_class_mock', autouse=True)
def _request_class_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'Request') as mock:
        yield mock


@pytest.fixture(name='request_mock', autouse=True)
def _request_mock(request_class_mock: MagicMock):
    return request_class_mock.return_value


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
        request_class_mock: MagicMock,
        get_client_from_twitter_ads_api_mock: MagicMock
    ):
        get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1
        )
        request_class_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource=RESOURCE,
            params=None
        )

    def test_should_pass_params_to_request_if_params_defined(
        self,
        request_class_mock: MagicMock,
        get_client_from_twitter_ads_api_mock: MagicMock
    ):
        get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1,
            PARAMS
        )
        request_class_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource=RESOURCE,
            params=PARAMS
        )

    def test_should_return_response_json_with_provenance(
        self,
        get_client_from_twitter_ads_api_mock: MagicMock,
        request_mock: MagicMock
    ):
        get_client_from_twitter_ads_api_mock.return_value = 'client'
        response_mock = request_mock.perform.return_value
        response_mock.body = RESPONSE_JSON_1
        actual_response_json = get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1
        )
        assert actual_response_json == {**RESPONSE_JSON_1, 'provenance': PROVENANCE_1}

    def test_should_return_response_json_with_provenance_include_params_if_defined(
        self,
        get_client_from_twitter_ads_api_mock: MagicMock,
        request_mock: MagicMock
    ):
        get_client_from_twitter_ads_api_mock.return_value = 'client'
        response_mock = request_mock.perform.return_value
        response_mock.body = RESPONSE_JSON_1
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
    def test_should_pass_none_to_params_dict_if_param_names_and_param_from_bq_not_exist(
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

    def test_should_pass_params_dict_if_param_names_and_bigquery_are_defined_in_source_conf(
        self,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = (
            PARAM_VALUES_FROM_BQ
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_PARAM_NAMES_AND_BQ
        ))
        today = date.today()
        yesterday = today - timedelta(days=1)
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_PARAM_NAMES_AND_BQ,
            params_dict={
                PARAM_NAMES[0]: PARAM_VALUES_FROM_BQ[0],
                PARAM_NAMES[1]: '2018-05-01',
                PARAM_NAMES[2]: yesterday.isoformat()
            }
        )
