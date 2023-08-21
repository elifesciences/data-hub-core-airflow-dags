from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchSourceConfig,
    BigQueryToOpenSearchTargetConfig,
    OpenSearchTargetConfig
)
import data_pipeline.opensearch.bigquery_to_opensearch_pipeline as test_module
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    fetch_documents_from_bigquery_and_load_into_opensearch,
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list,
    get_opensearch_client,
    load_documents_into_opensearch
)


BIGQUERY_TO_OPENSEARCH_CONFIG_1 = BigQueryToOpenSearchConfig(
    source=BigQueryToOpenSearchSourceConfig(
        bigquery=BigQuerySourceConfig(
            project_name='project1',
            sql_query='query 1'
        )
    ),
    target=BigQueryToOpenSearchTargetConfig(
        opensearch=OpenSearchTargetConfig(
            hostname='hostname1',
            port=9200,
            username='username1',
            password='password1',
            index_name='index_1'
        )
    )
)

OPENSEARCH_TARGET_CONFIG_1 = BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch


@pytest.fixture(name='opensearch_class_mock', autouse=True)
def _opensearch_class_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'OpenSearch') as mock:
        yield mock


@pytest.fixture(name='opensearch_mock')
def _opensearch_mock(opensearch_class_mock: MagicMock) -> MagicMock:
    return opensearch_class_mock.return_value


@pytest.fixture(name='iter_documents_from_bigquery_mock')
def _iter_documents_from_bigquery_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'iter_documents_from_bigquery') as mock:
        yield mock


@pytest.fixture(name='get_opensearch_client_mock')
def _get_opensearch_client_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'get_opensearch_client') as mock:
        yield mock


@pytest.fixture(name='load_documents_into_opensearch_mock')
def _load_documents_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'load_documents_into_opensearch') as mock:
        yield mock


@pytest.fixture(name='fetch_documents_from_bigquery_and_load_into_opensearch_mock')
def _fetch_documents_from_bigquery_and_load_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(
        test_module,
        'fetch_documents_from_bigquery_and_load_into_opensearch'
    ) as mock:
        yield mock


class TestGetOpenSearchClient:
    def test_should_pass_hostname_and_port_to_opensearch_class(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['hosts'] == [{
            'host': OPENSEARCH_TARGET_CONFIG_1.hostname,
            'port': OPENSEARCH_TARGET_CONFIG_1.port
        }]

    def test_should_connect_via_ssl(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['use_ssl'] is True

    def test_should_pass_username_and_password_to_opensearch_class(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['http_auth'] == (
            OPENSEARCH_TARGET_CONFIG_1.username,
            OPENSEARCH_TARGET_CONFIG_1.password
        )

    def test_should_not_include_secrets_in_repr_or_str_output(
        self
    ):
        client = get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        text = f'repr={repr(client)}, str={str(client)}'
        assert OPENSEARCH_TARGET_CONFIG_1.username not in text
        assert OPENSEARCH_TARGET_CONFIG_1.password not in text

    def test_should_return_opensearch_instance(
        self,
        opensearch_class_mock: MagicMock
    ):
        client = get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        assert client == opensearch_class_mock.return_value


class TestLoadDocumentsIntoOpenSearch:
    def test_should_pass_opensearch_target_config_to_get_cient(
        self,
        get_opensearch_client_mock: MagicMock
    ):
        load_documents_into_opensearch(
            [],
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1
        )
        get_opensearch_client_mock.assert_called_with(OPENSEARCH_TARGET_CONFIG_1)


class TestFetchDocumentsFromBigQueryAndLoadIntoOpenSearch:
    def test_should_fetch_documents_from_bigquery_and_pass_to_opensearch(
        self,
        iter_documents_from_bigquery_mock: MagicMock,
        load_documents_into_opensearch_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_load_into_opensearch(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        iter_documents_from_bigquery_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1.source.bigquery
        )
        load_documents_into_opensearch_mock.assert_called_with(
            iter_documents_from_bigquery_mock.return_value,
            opensearch_target_config=BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch
        )


class TestFetchDocumentsFromBigQueryAndLoadIntoOpenSearchFromConfigList:
    def test_should_process_each_config_item(
        self,
        fetch_documents_from_bigquery_and_load_into_opensearch_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list([
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        ])
        fetch_documents_from_bigquery_and_load_into_opensearch_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
