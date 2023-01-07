from datetime import datetime

from data_pipeline.generic_web_api.url_builder import (
    get_url_builder_class,
    DynamicBioRxivURLBuilder,
    UrlComposeParam
)


TEST_BIORXIV_API_URL = 'https://test.api.biorxiv.org/details/server'


class TestGetUrlBuilderClass:
    def test_should_return_biorxiv_api_class(self):
        url_builder_class = get_url_builder_class('biorxiv_api')
        assert url_builder_class == DynamicBioRxivURLBuilder


class TestDynamicBioRxivURLBuilder:
    def test_should_include_interval_and_offset_in_url(self):
        url_builder = DynamicBioRxivURLBuilder(
            url_excluding_configurable_parameters=TEST_BIORXIV_API_URL,
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
