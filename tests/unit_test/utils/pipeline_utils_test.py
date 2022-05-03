from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

import google.cloud.exceptions

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.utils.pipeline_utils import (
    fetch_single_column_value_list_for_bigquery_source_config,
    get_response_json_with_provenance_from_api
)
from data_pipeline.utils import (
    pipeline_utils as pipeline_utils_module
)


BIGQUERY_SOURCE_CONFIG_1 = BigQuerySourceConfig(
    project_name='project1',
    sql_query='query1'
)


API_URL_1 = '/api1'
API_PARAMS_1 = {'param1': 'value1'}

SINGLE_ITEM_RESPONSE_JSON_1 = {
    'data': 'value1'
}

MOCK_UTC_NOW_STR = '2021-01-02T12:34:56'


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(pipeline_utils_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


@pytest.fixture(name='requests_mock', autouse=True)
def _requests_mock():
    with patch.object(pipeline_utils_module, 'requests') as mock:
        yield mock


@pytest.fixture(name='get_single_column_value_list_from_bq_query_mock', autouse=True)
def _get_single_column_value_list_from_bq_query_mock():
    with patch.object(
        pipeline_utils_module,
        'get_single_column_value_list_from_bq_query'
    ) as mock:
        yield mock


class TestFetchSingleColumnValueListForBigQuerySourceConfig:
    def test_should_call_get_single_column_value_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config(BIGQUERY_SOURCE_CONFIG_1)
        get_single_column_value_list_from_bq_query_mock.assert_called_with(
            project_name=BIGQUERY_SOURCE_CONFIG_1.project_name,
            query=BIGQUERY_SOURCE_CONFIG_1.sql_query
        )

    def test_should_return_doi_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        get_single_column_value_list_from_bq_query_mock.return_value = [
            'value1', 'value2'
        ]
        actual_doi_list = fetch_single_column_value_list_for_bigquery_source_config(
            BIGQUERY_SOURCE_CONFIG_1
        )
        assert actual_doi_list == get_single_column_value_list_from_bq_query_mock.return_value

    def test_should_fail_if_not_found_exception_and_not_ignored(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        get_single_column_value_list_from_bq_query_mock.side_effect = (
            google.cloud.exceptions.NotFound('not found')
        )
        with pytest.raises(google.cloud.exceptions.NotFound):
            fetch_single_column_value_list_for_bigquery_source_config(
                BIGQUERY_SOURCE_CONFIG_1._replace(
                    ignore_not_found=False
                )
            )

    def test_should_return_empty_list_if_not_found_exception_and_ignored(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        get_single_column_value_list_from_bq_query_mock.side_effect = (
            google.cloud.exceptions.NotFound('not found')
        )
        result = fetch_single_column_value_list_for_bigquery_source_config(
            BIGQUERY_SOURCE_CONFIG_1._replace(
                ignore_not_found=True
            )
        )
        assert result == []


class TestGetResponseJsonWithProvenanceFromApi:
    def test_should_pass_url_and_params_to_requests_get(
        self,
        requests_mock: MagicMock
    ):
        get_response_json_with_provenance_from_api(
            API_URL_1,
            params=API_PARAMS_1
        )
        requests_mock.get.assert_called_with(
            API_URL_1,
            params=API_PARAMS_1
        )

    def test_should_pass_url_and_params_to_session_get_if_provided(
        self
    ):
        session_mock = MagicMock(name='session')
        get_response_json_with_provenance_from_api(
            API_URL_1,
            params=API_PARAMS_1,
            session=session_mock
        )
        session_mock.get.assert_called_with(
            API_URL_1,
            params=API_PARAMS_1
        )

    def test_should_return_response_json(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.json.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        actual_response_json = get_response_json_with_provenance_from_api(
            API_URL_1,
            params=API_PARAMS_1
        )
        actual_response_without_provenance_json = {
            key: value
            for key, value in actual_response_json.items()
            if key != 'provenance'
        }
        assert actual_response_without_provenance_json == SINGLE_ITEM_RESPONSE_JSON_1

    def test_should_include_provenance(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.json.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        response_mock.status_code = 200
        actual_response_json = get_response_json_with_provenance_from_api(
            API_URL_1,
            params=API_PARAMS_1
        )
        provenance_json = actual_response_json['provenance']
        assert provenance_json['api_url'] == API_URL_1
        assert provenance_json['request_url'] == response_mock.url
        assert provenance_json['http_status'] == 200

    def test_should_extend_provenance(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.json.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        passed_in_provenance = {'imported_timestamp': MOCK_UTC_NOW_STR}
        actual_response_json = get_response_json_with_provenance_from_api(
            API_URL_1,
            params=API_PARAMS_1,
            provenance=passed_in_provenance
        )
        provenance_json = actual_response_json['provenance']
        assert provenance_json['api_url'] == API_URL_1
        assert provenance_json['imported_timestamp'] == MOCK_UTC_NOW_STR