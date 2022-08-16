from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
import logging

from data_pipeline.twitter_ads_api import (
    twitter_ads_api_pipeline as twitter_ads_api_pipeline_module
)

from data_pipeline.twitter_ads_api.twitter_ads_api_pipeline import (
    get_param_dict_from_api_query_parameters,
    get_provenance,
    get_bq_compatible_json_response_from_resource_with_provenance,
    iter_bq_compatible_json_response_from_resource_with_provenance
)

from data_pipeline.utils.pipeline_config import (
    BigQueryTargetConfig
)

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiApiQueryParametersConfig,
    TwitterAdsApiParameterNamesForConfig,
    TwitterAdsApiParameterValuesConfig,
    TwitterAdsApiSourceConfig
)

LOGGER = logging.getLogger(__name__)

RESOURCE = 'resource_1'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SOURCE_CONFIG_1 = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS
)

FROM_BIGQUERY_PARAM_VALUE = ['bq_param_1']
START_TIME_PARAM_VALUE = 'start_time_value_1'
END_TIME_PARAM_VALUE = 'end_time_value_1'
PLACEMENT_PARAM_VALUE = ['placement_value_1']

PARAM_NAME_FOR_BIGQUERY_VALUE = 'param_name_for_bq_value_1'
PARAM_NAME_FOR_START_TIME = 'param_name_for_start_time_1'
PARAM_NAME_FOR_END_TIME = 'param_name_for_end_time_1'
PARAM_NAME_FOR_PLACEMENT = 'param_name_for_placement_1'

PARAMETER_NAMES_FOR = TwitterAdsApiParameterNamesForConfig(
    bigquery_value=PARAM_NAME_FOR_BIGQUERY_VALUE,
    start_time=PARAM_NAME_FOR_START_TIME,
    end_time=PARAM_NAME_FOR_END_TIME
)

PARAMETER_NAMES_FOR_WITH_PLACEMENT = TwitterAdsApiParameterNamesForConfig(
    bigquery_value=PARAM_NAME_FOR_BIGQUERY_VALUE,
    start_time=PARAM_NAME_FOR_START_TIME,
    end_time=PARAM_NAME_FOR_END_TIME,
    placement=PARAM_NAME_FOR_PLACEMENT
)

PARAMETER_VALUES = TwitterAdsApiParameterValuesConfig(
    from_bigquery=FROM_BIGQUERY_PARAM_VALUE,
    start_time_value=START_TIME_PARAM_VALUE,
    end_time_value=END_TIME_PARAM_VALUE,
)

PARAMETER_VALUES_WITH_PLACEMENT = TwitterAdsApiParameterValuesConfig(
    from_bigquery=FROM_BIGQUERY_PARAM_VALUE,
    start_time_value=START_TIME_PARAM_VALUE,
    end_time_value=END_TIME_PARAM_VALUE,
    placement_value=PLACEMENT_PARAM_VALUE
)

API_QUERY_PARAMETERS = TwitterAdsApiApiQueryParametersConfig(
    parameter_values=PARAMETER_VALUES,
    parameter_names_for=PARAMETER_NAMES_FOR
)

API_QUERY_PARAMETERS_WITH_PLACEMENT = TwitterAdsApiApiQueryParametersConfig(
    parameter_values=PARAMETER_VALUES_WITH_PLACEMENT,
    parameter_names_for=PARAMETER_NAMES_FOR_WITH_PLACEMENT
)

SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS,
    api_query_parameters=API_QUERY_PARAMETERS
)

SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_IN_PLACEMENT = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS,
    api_query_parameters=API_QUERY_PARAMETERS_WITH_PLACEMENT
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

API_QUERY_PARAMETERS_DICT = {
    'apiQueryParameterName': 'api_query_parameter_value_1'
}


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


@pytest.fixture(name='get_yesterdays_date_mock', autouse=True)
def _get_yesterdays_date_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'get_yesterdays_date') as mock:
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


@pytest.fixture(
    name='get_param_dict_from_api_query_parameters_mock',
    autouse=True
)
def _get_param_dict_from_api_query_parameters_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'get_param_dict_from_api_query_parameters'
    ) as mock:
        yield mock


class TestGetProvenance:
    def test_should_return_provenance_dict(self):
        actual_return_dict = get_provenance(source_config=SOURCE_CONFIG_1)
        assert actual_return_dict == PROVENANCE_1

    def test_should_return_provenance_dict_with_requested_params_if_params_dict_defined(self):
        actual_return_dict = get_provenance(
            source_config=SOURCE_CONFIG_1, params_dict=API_QUERY_PARAMETERS_DICT
        )
        assert actual_return_dict == {
            **PROVENANCE_1,
            'request_params': [{
                'name': 'apiQueryParameterName',
                'value': API_QUERY_PARAMETERS_DICT['apiQueryParameterName']
            }]
        }


class TestGetParamDictFromApiQueryParameters:
    def test_should_return_param_dict(self):
        actual_return_value = get_param_dict_from_api_query_parameters(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            value_from_bq=FROM_BIGQUERY_PARAM_VALUE,
            start_time=START_TIME_PARAM_VALUE,
            end_time=END_TIME_PARAM_VALUE
        )
        assert actual_return_value == {
            API_QUERY_PARAMETERS.parameter_names_for.bigquery_value: FROM_BIGQUERY_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.start_time: START_TIME_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.end_time: END_TIME_PARAM_VALUE
        }

    def test_should_return_param_dict_with_placement_if_defined(self):
        actual_return_value = get_param_dict_from_api_query_parameters(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            value_from_bq=FROM_BIGQUERY_PARAM_VALUE,
            start_time=START_TIME_PARAM_VALUE,
            end_time=END_TIME_PARAM_VALUE,
            placement=PLACEMENT_PARAM_VALUE[0]
        )
        assert actual_return_value == {
            API_QUERY_PARAMETERS.parameter_names_for.bigquery_value: FROM_BIGQUERY_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.start_time: START_TIME_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.end_time: END_TIME_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.placement: PLACEMENT_PARAM_VALUE[0]
        }


class TestGetBqCompatibleJsonResponseFromResourceWithProvenance:
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
            API_QUERY_PARAMETERS_DICT
        )
        request_class_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource=RESOURCE,
            params=API_QUERY_PARAMETERS_DICT
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


class TestIterBqCompatibleJsonResponseFromResourceWithProvenance:
    def test_should_pass_none_to_params_dict_if_api_query_parameters_not_defined(
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

    def test_should_pass_params_dict_if_api_query_parameters_defined(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = (
            API_QUERY_PARAMETERS
        )
        get_param_dict_from_api_query_parameters_mock.return_value = (
            API_QUERY_PARAMETERS_DICT
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
            params_dict=API_QUERY_PARAMETERS_DICT
        )

    def test_should_pass_correct_values_to_get_param_dict_func_if_api_query_parameters_defined(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_yesterdays_date_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = (
            FROM_BIGQUERY_PARAM_VALUE
        )
        get_yesterdays_date_mock.return_value = datetime(2022, 8, 3).date()
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS
        ))
        get_param_dict_from_api_query_parameters_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            value_from_bq=FROM_BIGQUERY_PARAM_VALUE[0],
            start_time=START_TIME_PARAM_VALUE,
            end_time='2022-08-03'
        )

    def test_sould_pass_params_dict_with_placement_defined(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = (
            API_QUERY_PARAMETERS_WITH_PLACEMENT
        )
        get_param_dict_from_api_query_parameters_mock.return_value = (
            API_QUERY_PARAMETERS_DICT
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_IN_PLACEMENT
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_IN_PLACEMENT,
            params_dict=API_QUERY_PARAMETERS_DICT
        )

    def test_should_pass_correct_values_to_get_param_dict_func_if_placement_defined(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = (
            FROM_BIGQUERY_PARAM_VALUE
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_IN_PLACEMENT
        ))
        get_param_dict_from_api_query_parameters_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS_WITH_PLACEMENT,
            value_from_bq=FROM_BIGQUERY_PARAM_VALUE[0],
            start_time=START_TIME_PARAM_VALUE,
            end_time=END_TIME_PARAM_VALUE,
            placement=PLACEMENT_PARAM_VALUE[0]
        )
