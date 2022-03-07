from typing import Sequence
from unittest.mock import MagicMock, patch

import pytest
from data_pipeline.europepmc.europepmc_config import EuropePmcSearchConfig, EuropePmcSourceConfig

import data_pipeline.europepmc.europepmc_etl as europepmc_etl_module
from data_pipeline.europepmc.europepmc_etl import (
    get_article_response_json_from_api,
    get_request_params_for_source_config,
    iter_article_data,
    iter_article_data_from_response_json
)


DOI_1 = 'doi1'


ITEM_RESPONSE_JSON_1 = {
    'doi': DOI_1
}

SINGLE_ITEM_RESPONSE_JSON_1 = {
    'resultList': {
        'result': [
            ITEM_RESPONSE_JSON_1
        ]
    }
}


API_URL_1 = 'https://api1'


SOURCE_CONFIG_1 = EuropePmcSourceConfig(
    api_url=API_URL_1,
    search=EuropePmcSearchConfig(
        query='query1'
    ),
    fields_to_return=['title', 'doi']
)


@pytest.fixture(name='get_article_response_json_from_api_mock')
def _get_article_response_json_from_api_mock():
    with patch.object(europepmc_etl_module, 'get_article_response_json_from_api') as mock:
        yield mock


@pytest.fixture(name='requests_mock')
def _requests_mock():
    with patch.object(europepmc_etl_module, 'requests') as mock:
        yield mock


def get_response_json_for_items(items: Sequence[str]) -> dict:
    return {
        'resultList': {
            'result': items
        }
    }


class TestIterArticleDataFromResponseJson:
    def test_should_return_single_item_from_response(self):
        result = list(iter_article_data_from_response_json(SINGLE_ITEM_RESPONSE_JSON_1))
        assert result == [ITEM_RESPONSE_JSON_1]


class TestGetRequestParamsForSourceConfig:
    def test_should_include_query_from_config(self):
        params = get_request_params_for_source_config(SOURCE_CONFIG_1)
        assert params['query'] == SOURCE_CONFIG_1.search.query

    def test_should_specify_format_json(self):
        params = get_request_params_for_source_config(SOURCE_CONFIG_1)
        assert params['format'] == 'json'

    def test_should_specify_result_type_core(self):
        params = get_request_params_for_source_config(SOURCE_CONFIG_1)
        assert params['resultType'] == 'core'


class TestGetArticleResponseJsonFromApi:
    def test_should_pass_url_and_params_to_requests_get(
        self,
        requests_mock: MagicMock
    ):
        get_article_response_json_from_api(SOURCE_CONFIG_1)
        requests_mock.get.assert_called_with(
            API_URL_1,
            params=get_request_params_for_source_config(SOURCE_CONFIG_1)
        )

    def test_should_return_response_json(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.json.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        actual_response_json = get_article_response_json_from_api(SOURCE_CONFIG_1)
        assert actual_response_json == SINGLE_ITEM_RESPONSE_JSON_1


class TestIterArticleData:
    def test_should_call_api_and_return_articles_from_response(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        result = list(iter_article_data(SOURCE_CONFIG_1))
        assert result == [ITEM_RESPONSE_JSON_1]
        get_article_response_json_from_api_mock.assert_called_with(SOURCE_CONFIG_1)

    def test_should_filter_returned_response_fields(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            {
                'doi': DOI_1,
                'other': 'other1'
            }
        ])
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=['doi'])
        ))
        assert result == [{
            'doi': DOI_1
        }]

    def test_should_not_filter_returned_response_fields_if_fields_to_return_is_none(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        item_response_1 = {
            'doi': DOI_1,
            'other': 'other1'
        }
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            item_response_1
        ])
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=None)
        ))
        assert result == [item_response_1]
