from datetime import datetime
import logging
from urllib.parse import parse_qs, urlparse

from data_pipeline.generic_web_api.request_builder import (
    CrossrefMetadataWebApiDynamicRequestBuilder,
    S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder,
    WebApiDynamicRequestBuilder,
    get_web_api_request_builder_class,
    BioRxivWebApiDynamicRequestBuilder,
    WebApiDynamicRequestParameters
)
from data_pipeline.utils.web_api import (
    DEFAULT_WEB_API_RETRY_CONFIG,
    DISABLED_WEB_API_RETRY_CONFIG
)


LOGGER = logging.getLogger(__name__)


TEST_API_URL_1 = 'https://test/api1'


class TestWebApiDynamicRequestBuilder:
    def test_should_enable_retry_and_not_allow_next_page_cursor(self):
        dynamic_request_builder = WebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            next_page_cursor='cursor',
            static_parameters={}
        )
        assert dynamic_request_builder.retry_config == DEFAULT_WEB_API_RETRY_CONFIG
        assert not dynamic_request_builder.allow_same_next_page_cursor


class TestDynamicBioRxivMedRxivURLBuilder:
    def test_should_initialize_dummy_parameteres(self):
        dynamic_request_builder = BioRxivWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            static_parameters={}
        )
        assert dynamic_request_builder.from_date_param
        assert dynamic_request_builder.to_date_param
        assert dynamic_request_builder.offset_param

    def test_should_include_interval_and_offset_in_url(self):
        dynamic_request_builder = BioRxivWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            static_parameters={}
        )
        url = dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                from_date=datetime.fromisoformat('2001-01-01'),
                to_date=datetime.fromisoformat('2001-01-02'),
                page_offset=10
            )
        )
        assert url == (
            dynamic_request_builder.url_excluding_configurable_parameters
            + '/2001-01-01/2001-01-02/10'
        )


class TestCrossrefMetadataWebApiDynamicRequestBuilder:
    def test_should_disable_retry_and_allow_next_page_cursor(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            next_page_cursor='cursor',
            static_parameters={}
        )
        assert dynamic_request_builder.retry_config == DISABLED_WEB_API_RETRY_CONFIG
        assert dynamic_request_builder.allow_same_next_page_cursor

    def test_should_pass_cursor_value_to_url(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            next_page_cursor='cursor',
            static_parameters={}
        )
        url = urlparse(dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                cursor='cursor-1'
            )
        ))
        params = parse_qs(url.query)
        assert params.get('cursor') == ['cursor-1']

    def test_should_pass_asterisk_as_initial_cursor(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            next_page_cursor='cursor',
            static_parameters={}
        )
        url = urlparse(dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                cursor=None
            )
        ))
        params = parse_qs(url.query)
        assert params.get('cursor') == ['*']

    def test_should_pass_from_date_as_filter_parameter(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            from_date_param='from-index-date',
            date_format=r'%Y-%m-%d',
            static_parameters={}
        )
        LOGGER.debug('dynamic_request_builder: %r', dynamic_request_builder)
        url = urlparse(dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                from_date=datetime.fromisoformat('2024-01-29+00:00')
            )
        ))
        LOGGER.debug('url: %r', url)
        params = parse_qs(url.query)
        assert params.get('filter') == ['from-index-date:2024-01-29']

    def test_should_pass_from_and_to_date_as_filter_parameter(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            from_date_param='from-index-date',
            to_date_param='until-index-date',
            date_format=r'%Y-%m-%d',
            static_parameters={}
        )
        LOGGER.debug('dynamic_request_builder: %r', dynamic_request_builder)
        url = urlparse(dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                from_date=datetime.fromisoformat('2024-01-29+00:00'),
                to_date=datetime.fromisoformat('2024-01-30+00:00')
            )
        ))
        LOGGER.debug('url: %r', url)
        params = parse_qs(url.query)
        assert params.get('filter') == ['from-index-date:2024-01-29,until-index-date:2024-01-30']

    def test_should_add_from_date_as_filter_parameter_to_existing_filter_parameter_in_url(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=(
                'https://test/api1?filter=existing-filter1:value1'
            ),
            from_date_param='from-index-date',
            date_format=r'%Y-%m-%d',
            static_parameters={}
        )
        LOGGER.debug('dynamic_request_builder: %r', dynamic_request_builder)
        url = urlparse(dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                from_date=datetime.fromisoformat('2024-01-29+00:00')
            )
        ))
        LOGGER.debug('url: %r', url)
        params = parse_qs(url.query)
        assert params.get('filter') == ['existing-filter1:value1,from-index-date:2024-01-29']

    def test_should_replace_placeholders(self):
        dynamic_request_builder = CrossrefMetadataWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1 + '/{placeholder}',
            static_parameters={}
        )
        url = dynamic_request_builder.get_url(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
                placeholder_values={'placeholder': 'buddy1'}
            )
        )
        LOGGER.debug('url: %r', url)
        assert url.rstrip('?') == TEST_API_URL_1 + '/buddy1'


class TestDynamicS2TitleAbstractEmbeddingsURLBuilder:
    def test_should_set_method_to_post(self):
        dynamic_request_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            static_parameters={}
        )
        assert dynamic_request_builder.method == 'POST'

    def test_should_set_max_source_values_per_request_to_16(self):
        dynamic_request_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            static_parameters={}
        )
        assert dynamic_request_builder.max_source_values_per_request == 16

    def test_should_generate_json_data_for_source_values(self):
        dynamic_request_builder = S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(
            url_excluding_configurable_parameters=TEST_API_URL_1,
            static_parameters={}
        )
        assert dynamic_request_builder.get_json(
            dynamic_request_parameters=WebApiDynamicRequestParameters(
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
        dynamic_request_builder_class = get_web_api_request_builder_class('biorxiv_medrxiv_api')
        assert dynamic_request_builder_class == BioRxivWebApiDynamicRequestBuilder

    def test_should_return_s2_title_abstract_embeddings_api_class(self):
        dynamic_request_builder_class = get_web_api_request_builder_class(
            's2_title_abstract_embeddings_api'
        )
        assert dynamic_request_builder_class == S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder
