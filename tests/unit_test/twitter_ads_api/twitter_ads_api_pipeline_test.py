import logging
from datetime import date, datetime
from unittest.mock import ANY, patch, MagicMock, call

import pytest

from data_pipeline.twitter_ads_api import (
    twitter_ads_api_pipeline as twitter_ads_api_pipeline_module
)

from data_pipeline.twitter_ads_api.twitter_ads_api_pipeline import (
    fetch_twitter_ads_api_data_and_load_into_bq,
    fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders,
    get_param_dict_from_api_query_parameters,
    get_provenance,
    get_bq_compatible_json_response_from_resource_with_provenance,
    get_current_final_end_date,
    iter_bq_compatible_json_response_from_resource_with_provenance,
    get_end_date_value_of_batch_period
)

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiApiQueryParametersConfig,
    TwitterAdsApiConfig,
    TwitterAdsApiParameterNamesForConfig,
    TwitterAdsApiParameterValuesConfig,
    TwitterAdsApiSourceConfig
)

from tests.unit_test.utils.data_store.bq_data_service_test_utils import (
    create_load_given_json_list_data_from_tempdir_to_bq_mock
)

LOGGER = logging.getLogger(__name__)

RESOURCE = 'resource_1'

SECRETS = MappingConfig(
            mapping={'key1': 'value1', 'key2': 'value2'},
            printable_mapping={'key1': '***', 'key2': '***'}
        )

PLACEHOLDERS_1 = {'account_id': 'account_id_1'}

SOURCE_CONFIG_1 = TwitterAdsApiSourceConfig(
    resource=RESOURCE,
    secrets=SECRETS
)

FROM_BIGQUERY_PARAM_VALUE = BigQuerySourceConfig(
    project_name='project_name_1',
    sql_query='sql_query_1'
)
SINGLE_PLACEMENT_PARAM_VALUE = ['placement_value_1']
MAX_PERIOD_IN_DAYS = 10
PERIOD_BATCH_SIZE_IN_DAYS = 7

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
    max_period_in_days=MAX_PERIOD_IN_DAYS
)

PARAMETER_VALUES_WITH_PLACEMENT = TwitterAdsApiParameterValuesConfig(
    from_bigquery=FROM_BIGQUERY_PARAM_VALUE,
    max_period_in_days=MAX_PERIOD_IN_DAYS,
    placement_value=SINGLE_PLACEMENT_PARAM_VALUE,
    period_batch_size_in_days=PERIOD_BATCH_SIZE_IN_DAYS
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

CONFIG_1 = TwitterAdsApiConfig(
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)

RESPONSE_JSON_1: dict = {
    'response_key_1': 'response_value_1_1',
    'response_key_2': 'response_value_1_2'
}

RESPONSE_JSON_2 = {
    'response_key_1': 'response_value_2_1',
    'response_key_2': 'response_value_2_2'
}

MOCK_UTC_NOW_STR = '2022-08-03T16:35:56'

PROVENANCE_1 = {
    'imported_timestamp': MOCK_UTC_NOW_STR,
    'request_resource': RESOURCE
}

API_QUERY_PARAMETERS_DICT = {
    'apiQueryParameterName': 'api_query_parameter_value_1'
}
# api_query_parameters_config.parameter_values.from_bigquery
ENTITY_CREATION_DATE_1 = '2022-07-30'
START_DATE_1 = '2022-08-01'


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        mock.strptime = datetime.strptime
        yield mock


@pytest.fixture(name='get_todays_date_mock', autouse=True)
def _get_todays_date_mock():
    with patch.object(twitter_ads_api_pipeline_module, 'get_todays_date') as mock:
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
    name='get_current_final_end_date_mock',
    autouse=True
)
def _get_current_final_end_date_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'get_current_final_end_date'
    ) as mock:
        mock.return_value = date.fromisoformat('2022-08-10')
        yield mock


@pytest.fixture(
    name='load_given_json_list_data_from_tempdir_to_bq_mock',
    autouse=True
)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq',
        create_load_given_json_list_data_from_tempdir_to_bq_mock()
    ) as mock:
        yield mock


@pytest.fixture(
    name='iter_bq_compatible_json_response_from_resource_with_provenance_mock',
    autouse=True
)
def _iter_bq_compatible_json_response_from_resource_with_provenance_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'iter_bq_compatible_json_response_from_resource_with_provenance'
    ) as mock:
        yield mock


@pytest.fixture(
    name='fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock',
)
def _fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock():
    with patch.object(
        twitter_ads_api_pipeline_module,
        'fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders'
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
    def test_should_pass_resource_with_replaced_placeholders_to_request(
        self,
        request_class_mock: MagicMock,
        get_client_from_twitter_ads_api_mock: MagicMock
    ):
        get_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1._replace(resource='/resource/{placeholder}'),
            placeholders={'placeholder': 'replaced_placeholder'}
        )
        request_class_mock.assert_called_with(
            client=get_client_from_twitter_ads_api_mock(SOURCE_CONFIG_1),
            method="GET",
            resource='/resource/replaced_placeholder',
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


class TestGetCurrentFinalEndDate:
    def test_should_return_final_end_date_as_today_if_max_period_in_days_is_in_future(
        self,
        get_todays_date_mock: MagicMock
    ):
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                max_period_in_days=10  # in 10 days
            )
        )
        get_todays_date_mock.return_value = date.fromisoformat('2022-08-05')  # in 4 days
        actual_return_value = get_current_final_end_date(
            api_query_parameters_config=api_query_parameters,
            initial_start_date=date.fromisoformat('2022-08-01')
        )
        assert actual_return_value == date.fromisoformat('2022-08-05')

    def test_should_return_final_end_date_of_given_period_if_ending_period_days_is_before_today(
        self,
        get_todays_date_mock: MagicMock
    ):
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                max_period_in_days=10  # in 10 days
            )
        )
        get_todays_date_mock.return_value = date.fromisoformat('2022-08-31')  # in 30 days
        actual_return_value = get_current_final_end_date(
            api_query_parameters_config=api_query_parameters,
            initial_start_date=date.fromisoformat('2022-08-01')
        )
        assert actual_return_value == date.fromisoformat('2022-08-11')


class TestGetEndDateValueOfBatchPeriod:
    def test_should_return_period_end_date_as_end_date(self):
        actual_return_value = get_end_date_value_of_batch_period(
            start_date=date.fromisoformat('2022-08-01'),
            final_end_date=date.fromisoformat('2022-08-10'),
            batch_size_in_days=5  # '2022-08-06'
        )
        assert actual_return_value == date.fromisoformat('2022-08-06')

    def test_should_return_final_end_date_as_end_date_if_period_ends_in_future(self):
        actual_return_value = get_end_date_value_of_batch_period(
            start_date=date.fromisoformat('2022-08-01'),
            final_end_date=date.fromisoformat('2022-08-10'),
            batch_size_in_days=30  # '2022-08-31' period ends in the future
        )
        assert actual_return_value == date.fromisoformat('2022-08-10')


class TestIterBqCompatibleJsonResponseFromResourceWithProvenance:
    def test_should_call_iter_dict_from_bq_query_func_if_api_query_parameters_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock
    ):
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
            PLACEHOLDERS_1
        ))
        iter_dict_from_bq_query_for_bigquery_source_config_mock.assert_called_with(
            FROM_BIGQUERY_PARAM_VALUE,
            placeholders=PLACEHOLDERS_1
        )

    def test_should_pass_none_to_params_dict_if_api_query_parameters_not_defined(
        self,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock
    ):
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_1,
            PLACEHOLDERS_1
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_1,
            params_dict=None,
            placeholders=PLACEHOLDERS_1
        )

    def test_should_pass_params_dict_if_api_query_parameters_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': START_DATE_1
        }])
        get_param_dict_from_api_query_parameters_mock.return_value = (
            API_QUERY_PARAMETERS_DICT
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
            PLACEHOLDERS_1
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
            params_dict=API_QUERY_PARAMETERS_DICT,
            placeholders=PLACEHOLDERS_1
        )

    def test_should_pass_correct_values_to_get_param_dict_func_if_api_query_parameters_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_current_final_end_date_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': '2022-08-01'
        }])
        get_current_final_end_date_mock.return_value = date.fromisoformat('2022-08-03')
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS
        ))
        get_param_dict_from_api_query_parameters_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS,
            entity_id='id_1',
            start_date='2022-08-01',
            end_date='2022-08-03'
        )

    def test_sould_pass_params_dict_with_placement_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
        get_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': START_DATE_1
        }])
        get_param_dict_from_api_query_parameters_mock.return_value = (
            API_QUERY_PARAMETERS_DICT
        )
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE,
            PLACEHOLDERS_1
        ))
        get_bq_compatible_json_response_from_resource_with_provenance_mock.assert_called_with(
            source_config=SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE,
            params_dict=API_QUERY_PARAMETERS_DICT,
            placeholders=PLACEHOLDERS_1
        )

    def test_should_pass_correct_values_to_get_param_dict_func_if_placement_defined(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_current_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': '2022-08-01'
        }])
        get_current_final_end_date_mock.return_value = date.fromisoformat('2022-08-02')
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

    def test_should_pass_entity_creation_date_from_bq_to_get_current_final_end_date(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_current_final_end_date_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': START_DATE_1
        }])
        list(iter_bq_compatible_json_response_from_resource_with_provenance(
            SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE
        ))
        get_current_final_end_date_mock.assert_called_with(
            api_query_parameters_config=API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE,
            initial_start_date=date.fromisoformat(ENTITY_CREATION_DATE_1)
        )

    def test_should_call_get_param_dict_for_each_placement_value(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock,
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': ENTITY_CREATION_DATE_1,
            'start_date': START_DATE_1
        }])
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                placement_value=['placement_value_1', 'placement_value_2'],
                period_batch_size_in_days=1
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
                start_date=START_DATE_1,
                end_date='2022-08-02',
                placement='placement_value_1'
            ),
            call(
                api_query_parameters_config=api_query_parameters,
                entity_id='id_1',
                start_date=START_DATE_1,
                end_date='2022-08-02',
                placement='placement_value_2'
            )
        ], any_order=True)

    def test_should_call_func_for_every_seven_days_till_final_end_date(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock,
        get_current_final_end_date_mock: MagicMock,
        get_param_dict_from_api_query_parameters_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = ([{
            'entity_id': 'id_1',
            'entity_creation_date': '2022-08-01',
            'start_date': '2022-08-01'
        }])
        get_current_final_end_date_mock.return_value = date.fromisoformat('2022-08-17')
        api_query_parameters = API_QUERY_PARAMETERS_WITH_SINGLE_PLACEMENT_VALUE._replace(
            parameter_values=PARAMETER_VALUES_WITH_PLACEMENT._replace(
                max_period_in_days=30  # end_date is in future
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


class TestFetchTwitterAdsApiDataAndLoadIntoBqWithPlaceholders:
    def test_should_pass_project_dataset_and_table_to_bq_load_method(
        self,
        iter_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [RESPONSE_JSON_1]
        iter_bq_compatible_json_response_from_resource_with_provenance_mock.return_value = json_list
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=TARGET_CONFIG_1.project_name,
            dataset_name=TARGET_CONFIG_1.dataset_name,
            table_name=TARGET_CONFIG_1.table_name,
            json_list=ANY
        )

    def test_should_pass_json_list_to_bq_load_method(
        self,
        iter_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [
            RESPONSE_JSON_1,
            RESPONSE_JSON_2
        ]
        iter_bq_compatible_json_response_from_resource_with_provenance_mock.return_value = json_list
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        assert (
            load_given_json_list_data_from_tempdir_to_bq_mock.latest_call_json_list
            == json_list
        )

    def test_should_pass_batched_json_list_to_bq_load_method(
        self,
        iter_bq_compatible_json_response_from_resource_with_provenance_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [
            RESPONSE_JSON_1,
            RESPONSE_JSON_2
        ]
        iter_bq_compatible_json_response_from_resource_with_provenance_mock.return_value = json_list
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders(
            CONFIG_1._replace(batch_size=1)
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        assert load_given_json_list_data_from_tempdir_to_bq_mock.calls_json_lists == [
            [json_list[0]],
            [json_list[1]]
        ]


class TestFetchTwitterAdsApiDataAndLoadIntoBq:
    def test_should_call_the_function_with_empty_placeholder_when_account_ids_not_defined(
        self,
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock: MagicMock
    ):
        fetch_twitter_ads_api_data_and_load_into_bq(CONFIG_1)
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock.assert_called_with(
            config=CONFIG_1,
            placeholders={}
        )

    def test_should_call_the_function_with_multiple_account_ids_defined_in_config(
        self,
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock: MagicMock
    ):
        new_config = CONFIG_1._replace(
            source=CONFIG_1.source._replace(
                account_ids=['account_id_1', 'account_id_2']
            )
        )
        fetch_twitter_ads_api_data_and_load_into_bq(new_config)
        fetch_twitter_ads_api_data_and_load_into_bq_with_placeholders_mock.has_calls(call=[
            call(
                config=new_config,
                placeholders={'account_id': 'account_id_1'}
            ),
            call(
                config=new_config,
                placeholders={'account_id': 'account_id_2'}
            )
        ])
