from data_pipeline.generic_web_api.url_builder import (
    get_url_builder_class,
    DynamicBioRxivURLBuilder
)


class TestGetUrlBuilderClass:
    def test_should_return_biorxiv_api_class(self):
        url_builder_class = get_url_builder_class('biorxiv_api')
        assert url_builder_class == DynamicBioRxivURLBuilder
