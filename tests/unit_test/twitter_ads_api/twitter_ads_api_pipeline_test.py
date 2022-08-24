from datetime import datetime
from unittest.mock import patch, MagicMock, call
import pytest

from data_pipeline.twitter_ads_api import (
    twitter_ads_api_pipeline as twitter_ads_api_pipeline_module
)

from data_pipeline.twitter_ads_api.twitter_ads_api_pipeline import (
    get_param_dict_from_api_query_parameters,
    get_provenance,
    get_bq_compatible_json_response_from_resource_with_provenance,
    get_final_end_date,
    iter_bq_compatible_json_response_from_resource_with_provenance,
    get_end_date_value_of_period
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

RESOURCE = 'resource_1'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SOURCE_CONFIG_1 = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS
)

FROM_BIGQUERY_PARAM_VALUE = ['bq_param_1']
SINGLE_PLACEMENT_PARAM_VALUE = ['placement_value_1']
ENDING_PERIOD_PER_DAY_VALUE = 10

PARAM_NAME_FOR_BIGQUERY_VALUE = 'param_name_for_bq_value_1'
PARAM_NAME_FOR_START_TIME = 'param_name_for_start_date_1'
PARAM_NAME_FOR_END_TIME = 'param_name_for_end_date_1'
PARAM_NAME_FOR_PLACEMENT = 'param_name_for_placement_1'

PARAMETER_NAMES_FOR = TwitterAdsApiParameterNamesForConfig(
    entity_id=PARAM_NAME_FOR_BIGQUERY_VALUE,
    start_date=PARAM_NAME_FOR_START_TIME,
    end_date=PARAM_NAME_FOR_END_TIME
)

PARAMETER_NAMES_FOR_WITH_PLACEMENT = TwitterAdsApiParameterNamesForConfig(
    entity_id=PARAM_NAME_FOR_BIGQUERY_VALUE,
    start_date=PARAM_NAME_FOR_START_TIME,
    end_date=PARAM_NAME_FOR_END_TIME,
    placement=PARAM_NAME_FOR_PLACEMENT
)

PARAMETER_VALUES = TwitterAdsApiParameterValuesConfig(
    from_bigquery=FROM_BIGQUERY_PARAM_VALUE,
    ending_period_per_day=ENDING_PERIOD_PER_DAY_VALUE
)

PARAMETER_VALUES_WITH_PLACEMENT = TwitterAdsApiParameterValuesConfig(
    from_bigquery=FROM_BIGQUERY_PARAM_VALUE,
    ending_period_per_day=10,
    placement_value=SINGLE_PLACEMENT_PARAM_VALUE
)

API_QUERY_PARAMETERS = TwitterAdsApiApiQueryParametersConfig(
    parameter_values=PARAMETER_VALUES,
    parameter_names_for=PARAMETER_NAMES_FOR,
)

API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE = TwitterAdsApiApiQueryParametersConfig(
    parameter_values=PARAMETER_VALUES_WITH_PLACEMENT,
    parameter_names_for=PARAMETER_NAMES_FOR_WITH_PLACEMENT
)

SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS,
    api_query_parameters=API_QUERY_PARAMETERS
)

SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS,
    api_query_parameters=API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE
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
        mock.strptime = datetime.strptime
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
    name='get_param_dict_from_api_query_parameters_mock',
    autouse=True
)
def _get_param_dict_from_api_query_parameters_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'get_param_dict_from_api_query_parameters'
    ) as mock:
        yield mock


@pytest.fixture(
    name='iter_dict_from_bq_query_for_bigquery_source_config_mock',
    autouse=True
)
def _iter_dict_from_bq_query_for_bigquery_source_config_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'iter_dict_from_bq_query_for_bigquery_source_config'
    ) as mock:
        yield mock


@pytest.fixture(
    name='get_final_end_date_mock',
    autouse=True
)
def _get_final_end_date_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'get_final_end_date'
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
            entity_id=FROM_BIGQUERY_PARAM_VALUE,
            start_date='2022-08-01',
            end_date='2022-08-02'
        )
        assert actual_return_value == {
            API_QUERY_PARAMETERS.parameter_names_for.entity_id: FROM_BIGQUERY_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.start_date: '2022-08-01',
            API_QUERY_PARAMETERS.parameter_names_for.end_date: '2022-08-02'
        }

    def test_should_return_param_dict_with_placement_if_defined(self):
        actual_return_value = get_param_dict_from_api_query_parameters(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            entity_id=FROM_BIGQUERY_PARAM_VALUE,
            start_date='2022-08-01',
            end_date='2022-08-02',
            placement=SINGLE_PLACEMENT_PARAM_VALUE[0]
        )
        assert actual_return_value == {
            API_QUERY_PARAMETERS.parameter_names_for.entity_id: FROM_BIGQUERY_PARAM_VALUE,
            API_QUERY_PARAMETERS.parameter_names_for.start_date: '2022-08-01',
            API_QUERY_PARAMETERS.parameter_names_for.end_date: '2022-08-02',
            API_QUERY_PARAMETERS.parameter_names_for.placement: SINGLE_PLACEMENT_PARAM_VALUE[0]
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


class TestFinalEndDate:
    def test_should_return_final_end_date_as_yesterday_if_the_ending_period_days_is_in_future(
        self,
        get_yesterdays_date_mock: MagicMock
    ):
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                ending_period_per_day=10  # in 10 days
            )
        )
        get_yesterdays_date_mock.return_value = datetime(2022, 8, 5).date()  # in 4 days
        actual_return_value = get_final_end_date(
            api_query_parameters_config=api_query_parameters,
            start_date_value_from_bq='2022-08-01'
        )
        assert actual_return_value == datetime.strptime('2022-08-05', '%Y-%m-%d').date()

    def test_should_return_final_end_date_of_given_period_if_ending_period_days_is_before_yesterday(
        self,
        get_yesterdays_date_mock: MagicMock
    ):
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                ending_period_per_day=10  # in 10 days
            )
        )
        get_yesterdays_date_mock.return_value = datetime(2022, 8, 31).date()  # in 30 days
        actual_return_value = get_final_end_date(
            api_query_parameters_config=api_query_parameters,
            start_date_value_from_bq='2022-08-01'
        )
        assert actual_return_value == datetime.strptime('2022-08-11', '%Y-%m-%d').date()


class TestGetEndDateValueOfPeriod:
    def test_should_return_period_end_date_as_end_date(self):
        actual_return_value = get_end_date_value_of_period(
            start_date_value=datetime.strptime('2022-08-01', '%Y-%m-%d').date(),
            final_end_date_value=datetime.strptime('2022-08-10', '%Y-%m-%d').date(),
            days_in_period=5  # '2022-08-06'
        )
        assert actual_return_value == datetime.strptime('2022-08-06', '%Y-%m-%d').date()

    def test_should_return_final_end_date_as_end_date_if_period_ends_in_future(self):
        actual_return_value = get_end_date_value_of_period(
            start_date_value=datetime.strptime('2022-08-01', '%Y-%m-%d').date(),
            final_end_date_value=datetime.strptime('2022-08-10', '%Y-%m-%d').date(),
            days_in_period=30  # '2022-08-31' period ends in the future
        )
        assert actual_return_value == datetime.strptime('2022-08-10', '%Y-%m-%d').date()


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
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-02', '%Y-%m-%d').date()
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
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_final_end_date_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-02', '%Y-%m-%d').date()
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS
        ))
        get_param_dict_from_api_query_parameters_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            entity_id='id_1',
            start_date='2022-08-01',
            end_date='2022-08-02'
        )

    def test_sould_pass_params_dict_with_placement_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-10', '%Y-%m-%d').date()
        get_param_dict_from_api_query_parameters_mock.return_value = (
            API_QUERY_PARAMETERS_DICT
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE,
            params_dict=API_QUERY_PARAMETERS_DICT
        )

    def test_should_pass_correct_values_to_get_param_dict_func_if_placement_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-02', '%Y-%m-%d').date()
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE
        ))
        get_param_dict_from_api_query_parameters_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE,
            entity_id='id_1',
            start_date='2022-08-01',
            end_date='2022-08-02',
            placement=SINGLE_PLACEMENT_PARAM_VALUE[0]
        )

    def test_should_not_get_the_param_dict_when_the_entity_with_start_date_out_of_range(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2014-08-01'}]  # '2014-12-01' is out of range
        )
        assert not get_param_dict_from_api_query_parameters_mock.called

    def test_should_call_get_param_dict_for_each_placement_value(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_final_end_date_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )

        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                placement_value=['placement_value_1', 'placement_value_2']
            )
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-08', '%Y-%m-%d').date()
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
                api_query_parameters=api_query_parameters
            )
        ))
        get_param_dict_from_api_query_parameters_mock.assert_has_calls(calls=[
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date='2022-08-01',
                end_date='2022-08-08',
                placement='placement_value_1'
            ),
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date='2022-08-01',
                end_date='2022-08-08',
                placement='placement_value_2'
            )
        ], any_order=True)

    def test_should_call_func_for_every_seven_days_till_final_end_date(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = (
            [{'entity_id': 'id_1', 'start_date': '2022-08-01'}]
        )
        get_final_end_date_mock.return_value = datetime.strptime('2022-08-17', '%Y-%m-%d').date()
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                ending_period_per_day=30  # end_date is in future
            )
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
                api_query_parameters=api_query_parameters
            )
        ))

        get_param_dict_from_api_query_parameters_mock.assert_has_calls(calls=[
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date='2022-08-01',
                end_date='2022-08-08',
                placement=SINGLE_PLACEMENT_PARAM_VALUE[0]
            ),
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date='2022-08-08',
                end_date='2022-08-15',
                placement=SINGLE_PLACEMENT_PARAM_VALUE[0]
            ),
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date='2022-08-15',
                # end_date here is equal to final_end_date because of +7 days would be in future
                end_date='2022-08-17',
                placement=SINGLE_PLACEMENT_PARAM_VALUE[0]
            )
        ], any_order=True)
