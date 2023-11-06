from datetime import datetime

from data_pipeline.generic_web_api.url_builder import (
    DynamicS2TitleAbstractEmbeddingsURLBuilder,
    get_url_builder_class,
    DynamicBioRxivMedRxivURLBuilder,
    UrlComposeParam
)


TEST_API_URL_1 = 'https://test/api1'


class TestDynamicBioRxivMedRxivURLBuilder:
    def test_should_initialize_dummy_parameteres(self):
        url_builder = DynamicBioRxivMedRxivURLBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.from_date_param
        assert url_builder.to_date_param
        assert url_builder.offset_param

    def test_should_include_interval_and_offset_in_url(self):
        url_builder = DynamicBioRxivMedRxivURLBuilder(
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
    def test_should_generate_json_data_for_source_values(self):
        url_builder = DynamicS2TitleAbstractEmbeddingsURLBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            compose_able_url_key_val={}
        )
        assert url_builder.get_json(source_values=iter([{
            'paper_id': 'paper_id1',
            'title': 'Title 1',
            'abstract': 'Abstract 1'
        }])) == [{
            'paper_id': 'paper_id1',
            'title': 'Title 1',
            'abstract': 'Abstract 1'
        }]


class TestGetUrlBuilderClass:
    def test_should_return_biorxiv_api_class(self):
        url_builder_class = get_url_builder_class('biorxiv_medrxiv_api')
        assert url_builder_class == DynamicBioRxivMedRxivURLBuilder

    def test_should_return_s2_title_abstract_embeddings_api_class(self):
        url_builder_class = get_url_builder_class('s2_title_abstract_embeddings_api')
        assert url_builder_class == DynamicS2TitleAbstractEmbeddingsURLBuilder
