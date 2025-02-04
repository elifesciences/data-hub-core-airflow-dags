from data_pipeline.utils.web_api import (
    DEFAULT_WEB_API_RETRY_CONFIG,
    WebApiRetryConfig
)


class TestWebApiRetryConfig:
    def test_should_return_default_config_for_empty_dict(self):
        retry_config = WebApiRetryConfig.from_dict({})
        assert retry_config == DEFAULT_WEB_API_RETRY_CONFIG

    def test_should_return_default_config_for_none(self):
        retry_config = WebApiRetryConfig.from_optional_dict(None)
        assert retry_config == DEFAULT_WEB_API_RETRY_CONFIG

    def test_should_read_config(self):
        retry_config = WebApiRetryConfig.from_dict({
            'maxRetryCount': 123,
            'retryBackoffFactor': 0.123,
            'retryOnResponseStatusList': [1, 2, 3]
        })
        assert retry_config == WebApiRetryConfig(
            max_retry_count=123,
            retry_backoff_factor=0.123,
            retry_on_response_status_list=[1, 2, 3]
        )
