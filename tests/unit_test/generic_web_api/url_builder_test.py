from datetime import datetime

from data_pipeline.generic_web_api.url_builder import (
    S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder,
    get_web_api_request_builder_class,
    BioRxivWebApiDynamicRequestBuilder,
    UrlComposeParam
)


TEST_API_URL_1 = 'https://test/api1'


class TestDynamicBioRxivMedRxivURLBuilder:
    def test_should_initialize_dummy_parameteres(self):
        url_builder = BioRxivWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.from_date_param
        assert url_builder.to_date_param
        assert url_builder.offset_param

    def test_should_include_interval_and_offset_in_url(self):
        url_builder = BioRxivWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        url = url_builder.get_url(url_compose_param=UrlComposeParam(
            from_date=datetime.fromisoformat('2001-01-01'),
            to_date=datetime.fromisoformat('2001-01-02'),
            page_offset=10
        ))
        assert url == (
            url_builder.url_excluding_configurable_parameters
            + '/2001-01-01/2001-01-02/10'
        )


class TestDynamicS2TitleAbstractEmbeddingsURLBuilder:
    def test_should_set_method_to_post(self):
        url_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.method == 'POST'

    def test_should_set_max_source_values_per_request_to_16(self):
        url_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.max_source_values_per_request == 16

    def test_should_generate_json_data_for_source_values(self):
        url_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.get_json(
            url_compose_param=UrlComposeParam(
                source_values=iter([{
                    'paper_id': 'paper_id1',
                    'title': 'Title 1',
                    'abstract': 'Abstract 1'
                }])
            )
        ) == [{
            'paper_id': 'paper_id1',
            'title': 'Title 1',
            'abstract': 'Abstract 1'
        }]


class TestGetUrlBuilderClass:
    def test_should_return_biorxiv_api_class(self):
        url_builder_class = get_web_api_request_builder_class('biorxiv_medrxiv_api')
        assert url_builder_class == BioRxivWebApiDynamicRequestBuilder

    def test_should_return_s2_title_abstract_embeddings_api_class(self):
        url_builder_class = get_web_api_request_builder_class('s2_title_abstract_embeddings_api')
        assert url_builder_class == S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder
