from data_pipeline.europepmc.europepmc_etl import (
    iter_article_data_from_response_json
)


ITEM_RESPONSE_JSON_1 = {
    'doi': 'doi1'
}


class TestIterArticleDataFromResponseJson:
    def test_should_return_single_item_from_response(self):
        result = list(iter_article_data_from_response_json({
            'resultList': {
                'result': [
                    ITEM_RESPONSE_JSON_1
                ]
            }
        }))
        assert result == [ITEM_RESPONSE_JSON_1]
