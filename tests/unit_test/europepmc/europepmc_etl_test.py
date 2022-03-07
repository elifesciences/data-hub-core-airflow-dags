from unittest.mock import MagicMock, patch

import pytest
from data_pipeline.europepmc.europepmc_config import EuropePmcSearchConfig, EuropePmcSourceConfig

import data_pipeline.europepmc.europepmc_etl as europepmc_etl_module
from data_pipeline.europepmc.europepmc_etl import (
    iter_article_data,
    iter_article_data_from_response_json
)


ITEM_RESPONSE_JSON_1 = {
    'doi': 'doi1'
}

SINGLE_ITEM_RESPONSE_JSON_1 = {
    'resultList': {
        'result': [
            ITEM_RESPONSE_JSON_1
        ]
    }
}


SOURCE_CONFIG_1 = EuropePmcSourceConfig(
    search=EuropePmcSearchConfig(
        query='query1'
    ),
    fields_to_return=['title', 'doi']
)


@pytest.fixture(name='get_article_response_json_from_api_mock')
def _get_article_response_json_from_api_mock():
    with patch.object(europepmc_etl_module, 'get_article_response_json_from_api') as mock:
        yield mock


class TestIterArticleDataFromResponseJson:
    def test_should_return_single_item_from_response(self):
        result = list(iter_article_data_from_response_json(SINGLE_ITEM_RESPONSE_JSON_1))
        assert result == [ITEM_RESPONSE_JSON_1]


class TestIterArticleData:
    def test_should_call_api_and_return_articles_from_response(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        result = list(iter_article_data(SOURCE_CONFIG_1))
        assert result == [ITEM_RESPONSE_JSON_1]
        get_article_response_json_from_api_mock.assert_called_with(SOURCE_CONFIG_1)
