import dataclasses
from datetime import datetime
from typing import Iterator
from unittest.mock import ANY, MagicMock, call, patch

import pytest

import opensearchpy.serializer

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig, StateFileConfig
from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchFieldNamesForConfig,
    BigQueryToOpenSearchInitialStateConfig,
    BigQueryToOpenSearchSourceConfig,
    BigQueryToOpenSearchStateConfig,
    BigQueryToOpenSearchTargetConfig,
    OpenSearchTargetConfig
)
import data_pipeline.opensearch.bigquery_to_opensearch_pipeline as test_module
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    create_or_update_index_and_load_documents_into_opensearch,
    create_or_update_opensearch_index,
    fetch_documents_from_bigquery_and_load_into_opensearch,
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list,
    get_opensearch_client,
    get_wrapped_query,
    iter_documents_from_bigquery,
    iter_opensearch_bulk_action_for_documents,
    load_documents_into_opensearch
)


OPENSEARCH_INDEX_SETTNGS_WITHOUT_MAPPINGS_1: dict = {
    'settings': {'index': {'some_setting': 'value'}}
}

OPENSEARCH_INDEX_SETTNGS_WITH_MAPPINGS_1 = {
    **OPENSEARCH_INDEX_SETTNGS_WITHOUT_MAPPINGS_1,
    'mappings': {'properties': {'field1': {'type': 'text'}}}
}

OPENSEARCH_INDEX_SETTNGS_1 = OPENSEARCH_INDEX_SETTNGS_WITHOUT_MAPPINGS_1


ID_FIELD_NAME = 'id1'
TIMESTAMP_FIELD_NAME = 'timestamp1'


TIMESTAMP_1 = datetime.fromisoformat('2001-02-03+00:00')


QUERY_1 = 'query 1'


BIGQUERY_TO_OPENSEARCH_CONFIG_1 = BigQueryToOpenSearchConfig(
    source=BigQueryToOpenSearchSourceConfig(
        bigquery=BigQuerySourceConfig(
            project_name='project1',
            sql_query=QUERY_1
        )
    ),
    field_names_for=BigQueryToOpenSearchFieldNamesForConfig(
        id=ID_FIELD_NAME,
        timestamp=TIMESTAMP_FIELD_NAME
    ),
    target=BigQueryToOpenSearchTargetConfig(
        opensearch=OpenSearchTargetConfig(
            hostname='hostname1',
            port=9200,
            username='username1',
            password='password1',
            index_name='index_1'
        )
    ),
    state=BigQueryToOpenSearchStateConfig(
        initial_state=BigQueryToOpenSearchInitialStateConfig(
            start_timestamp=TIMESTAMP_1
        ),
        state_file=StateFileConfig(bucket_name='bucket1', object_name='object1')
    )
)

OPENSEARCH_TARGET_CONFIG_1 = BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch
FIELD_NAMES_FOR_CONFIG_1 = BIGQUERY_TO_OPENSEARCH_CONFIG_1.field_names_for


DOCUMENT_1 = {
    ID_FIELD_NAME: '10.12345/doi1',
    TIMESTAMP_FIELD_NAME: datetime.fromisoformat('2011-01-01+00:00')
}

DOCUMENT_2 = {
    ID_FIELD_NAME: '10.12345/doi2',
    TIMESTAMP_FIELD_NAME: datetime.fromisoformat('2012-01-01+00:00')
}

DOCUMENT_3 = {
    ID_FIELD_NAME: '10.12345/doi3',
    TIMESTAMP_FIELD_NAME: datetime.fromisoformat('2013-01-01+00:00')
}


@pytest.fixture(name='upload_s3_object_mock', autouse=True)
def _upload_s3_object_mock():
    with patch.object(test_module, 'upload_s3_object') as mock:
        yield mock


@pytest.fixture(name='save_state_to_s3_for_config_mock')
def _save_state_to_s3_for_config_mock():
    with patch.object(test_module, 'save_state_to_s3_for_config') as mock:
        yield mock


@pytest.fixture(name='load_state_or_default_from_s3_for_config_mock')
def _load_state_or_default_from_s3_for_config_mock():
    with patch.object(test_module, 'load_state_or_default_from_s3_for_config') as mock:
        yield mock


@pytest.fixture(name='opensearch_class_mock', autouse=True)
def _opensearch_class_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'OpenSearch') as mock:
        # using this, we can use the real streaming_bulk method
        mock.return_value.transport.serializer = opensearchpy.serializer.JSONSerializer()
        yield mock


@pytest.fixture(name='opensearch_client_mock')
def _opensearch_client_mock(opensearch_class_mock: MagicMock) -> MagicMock:
    return opensearch_class_mock.return_value


@pytest.fixture(name='streaming_bulk_mock')
def _streaming_bulk_mock() -> Iterator[MagicMock]:
    with patch('opensearchpy.helpers.streaming_bulk') as mock:
        yield mock


@pytest.fixture(name='iter_dict_from_bq_query_for_bigquery_source_config_mock', autouse=True)
def _iter_dict_from_bq_query_for_bigquery_source_config_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'iter_dict_from_bq_query_for_bigquery_source_config') as mock:
        yield mock


@pytest.fixture(name='iter_documents_from_bigquery_mock')
def _iter_documents_from_bigquery_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'iter_documents_from_bigquery') as mock:
        yield mock


@pytest.fixture(name='get_opensearch_client_mock')
def _get_opensearch_client_mock(opensearch_client_mock: MagicMock) -> Iterator[MagicMock]:
    with patch.object(test_module, 'get_opensearch_client') as mock:
        mock.return_value = opensearch_client_mock
        yield mock


@pytest.fixture(name='create_or_update_opensearch_index_mock')
def _create_or_update_opensearch_index_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'create_or_update_opensearch_index') as mock:
        yield mock


@pytest.fixture(name='iter_opensearch_bulk_action_for_documents_mock')
def _iter_opensearch_bulk_action_for_documents_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'iter_opensearch_bulk_action_for_documents') as mock:
        mock.side_effect = iter_opensearch_bulk_action_for_documents
        yield mock


@pytest.fixture(name='load_documents_into_opensearch_mock')
def _load_documents_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'load_documents_into_opensearch') as mock:
        yield mock


@pytest.fixture(name='create_or_update_index_and_load_documents_into_opensearch_mock')
def _create_or_update_index_and_load_documents_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(
        test_module,
        'create_or_update_index_and_load_documents_into_opensearch'
    ) as mock:
        yield mock


@pytest.fixture(name='fetch_documents_from_bigquery_and_load_into_opensearch_mock')
def _fetch_documents_from_bigquery_and_load_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(
        test_module,
        'fetch_documents_from_bigquery_and_load_into_opensearch'
    ) as mock:
        yield mock


class TestGetWrappedQuery:
    def test_should_add_order_by_and_where_clause(self):
        wrapped_query = get_wrapped_query(
            QUERY_1,
            timestamp_field_name='timestamp1',
            start_timestamp=TIMESTAMP_1
        )
        assert wrapped_query == '\n'.join([
            'SELECT * FROM (',
            QUERY_1,
            ')',
            f"WHERE timestamp1 >= TIMESTAMP('{TIMESTAMP_1.isoformat()}')",
            'ORDER BY timestamp1'
        ])


class TestIterDocumentsFromBigQuery:
    def test_should_pass_wrapped_query_to_iter_dict_from_bq_query_for_bigquery_source_config(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = [{
            'field1': 'value1'
        }]
        result = list(iter_documents_from_bigquery(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1,
            start_timestamp=TIMESTAMP_1
        ))
        iter_dict_from_bq_query_for_bigquery_source_config_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1.source.bigquery._replace(
                sql_query=get_wrapped_query(
                    QUERY_1,
                    timestamp_field_name=TIMESTAMP_FIELD_NAME,
                    start_timestamp=TIMESTAMP_1
                )
            )
        )
        assert result == [{'field1': 'value1'}]

    def test_should_return_an_iterable(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = iter([{
            'field1': 'value1'
        }])
        iterable = iter_documents_from_bigquery(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1,
            start_timestamp=TIMESTAMP_1
        )
        assert list(iterable)
        # iterable should now be consumed and empty
        assert not list(iterable)

    def test_should_remove_empty_embedding_vector_array(
        self,
        iter_dict_from_bq_query_for_bigquery_source_config_mock: MagicMock
    ):
        iter_dict_from_bq_query_for_bigquery_source_config_mock.return_value = [{
            'other_field': 'other_value',
            'empty_vector': []
        }]
        result = list(iter_documents_from_bigquery(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1,
            start_timestamp=TIMESTAMP_1
        ))
        assert result == [{
            'other_field': 'other_value'
        }]


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

    def test_should_pass_in_timeout(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['timeout'] == BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch.timeout

    def test_should_connect_via_ssl(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch)
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['use_ssl'] is True

    def test_should_verify_certificates_and_show_ssl_warnings_if_enabled(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(
            dataclasses.replace(
                BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch,
                verify_certificates=True
            )
        )
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['verify_certs'] is True
        assert kwargs['ssl_show_warn'] is True

    def test_should_not_verify_certificates_or_show_ssl_warnings_if_disabled(
        self,
        opensearch_class_mock: MagicMock
    ):
        get_opensearch_client(
            dataclasses.replace(
                BIGQUERY_TO_OPENSEARCH_CONFIG_1.target.opensearch,
                verify_certificates=False
            )
        )
        opensearch_class_mock.assert_called()
        _, kwargs = opensearch_class_mock.call_args
        assert kwargs['verify_certs'] is False
        assert kwargs['ssl_show_warn'] is False

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


class TestCreateOrUpdateOpenSearchIndex:
    def test_should_create_index_without_settings_if_it_does_not_exist(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = False
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                index_settings=None
            )
        )
        opensearch_client_mock.indices.create.assert_called_with(
            index=OPENSEARCH_TARGET_CONFIG_1.index_name,
            body=None
        )

    def test_should_create_index_with_settings_if_it_does_not_exist(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = False
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                index_settings=OPENSEARCH_INDEX_SETTNGS_1
            )
        )
        opensearch_client_mock.indices.create.assert_called_with(
            index=OPENSEARCH_TARGET_CONFIG_1.index_name,
            body=OPENSEARCH_INDEX_SETTNGS_1
        )

    def test_should_not_update_index_without_settings_if_it_does_exist(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = True
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                update_index_settings=True,
                index_settings=None
            )
        )
        opensearch_client_mock.indices.put_settings.assert_not_called()
        opensearch_client_mock.indices.put_mapping.assert_not_called()

    def test_should_not_update_index_or_mappings_if_not_enabled(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = True
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                update_index_settings=False,
                update_mappings=False,
                index_settings=OPENSEARCH_INDEX_SETTNGS_WITH_MAPPINGS_1
            )
        )
        opensearch_client_mock.indices.put_settings.assert_not_called()
        opensearch_client_mock.indices.put_mapping.assert_not_called()

    def test_should_update_index_with_settings_but_no_mappings(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = True
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                update_index_settings=True,
                index_settings=OPENSEARCH_INDEX_SETTNGS_WITHOUT_MAPPINGS_1
            )
        )
        opensearch_client_mock.indices.put_settings.assert_called_with(
            index=OPENSEARCH_TARGET_CONFIG_1.index_name,
            body=OPENSEARCH_INDEX_SETTNGS_WITHOUT_MAPPINGS_1
        )
        opensearch_client_mock.indices.put_mapping.assert_not_called()

    def test_should_update_index_with_settings_and_mappings(
        self,
        opensearch_client_mock: MagicMock
    ):
        opensearch_client_mock.indices.exists.return_value = True
        create_or_update_opensearch_index(
            client=opensearch_client_mock,
            opensearch_target_config=dataclasses.replace(
                OPENSEARCH_TARGET_CONFIG_1,
                update_index_settings=True,
                update_mappings=True,
                index_settings=OPENSEARCH_INDEX_SETTNGS_WITH_MAPPINGS_1
            )
        )
        opensearch_client_mock.indices.put_settings.assert_called_with(
            index=OPENSEARCH_TARGET_CONFIG_1.index_name,
            body=OPENSEARCH_INDEX_SETTNGS_WITH_MAPPINGS_1
        )
        opensearch_client_mock.indices.put_mapping.assert_called_with(
            index=OPENSEARCH_TARGET_CONFIG_1.index_name,
            body=OPENSEARCH_INDEX_SETTNGS_WITH_MAPPINGS_1['mappings']
        )


class TestIterOpenSearchBulkActionForDocuments:
    def test_should_wrap_document_with_bulk_index_action(self):
        bulk_actions = list(iter_opensearch_bulk_action_for_documents(
            [DOCUMENT_1],
            index_name='index_1',
            id_field_name=ID_FIELD_NAME
        ))
        assert bulk_actions == [{
            '_op_type': 'index',
            "_index": 'index_1',
            '_id': DOCUMENT_1[ID_FIELD_NAME],
            '_source': DOCUMENT_1
        }]


class TestLoadDocumentsIntoOpenSearch:
    def test_should_pass_documents_and_index_name_to_iter_opensearch_action_method(
        self,
        opensearch_client_mock: MagicMock,
        iter_opensearch_bulk_action_for_documents_mock: MagicMock
    ):
        load_documents_into_opensearch(
            [DOCUMENT_1],
            client=opensearch_client_mock,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1,
            field_names_for_config=FIELD_NAMES_FOR_CONFIG_1,
            batch_size=BIGQUERY_TO_OPENSEARCH_CONFIG_1.batch_size
        )
        iter_opensearch_bulk_action_for_documents_mock.assert_called_with(
            [DOCUMENT_1],
            index_name=OPENSEARCH_TARGET_CONFIG_1.index_name,
            id_field_name=ID_FIELD_NAME
        )

    def test_should_pass_client_and_batch_size_to_streaming_bulk_method(
        self,
        opensearch_client_mock: MagicMock,
        streaming_bulk_mock: MagicMock
    ):
        load_documents_into_opensearch(
            iter([DOCUMENT_1]),
            client=opensearch_client_mock,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1,
            field_names_for_config=FIELD_NAMES_FOR_CONFIG_1,
            batch_size=BIGQUERY_TO_OPENSEARCH_CONFIG_1.batch_size
        )
        streaming_bulk_mock.assert_called_with(
            client=opensearch_client_mock,
            actions=ANY,
            chunk_size=BIGQUERY_TO_OPENSEARCH_CONFIG_1.batch_size
        )

    def test_should_pass_bulk_actions_to_streaming_bulk_and_consume_documents(
        self,
        opensearch_client_mock: MagicMock,
        streaming_bulk_mock: MagicMock
    ):
        expected_bulk_actions = list(iter_opensearch_bulk_action_for_documents(
            [DOCUMENT_1],
            index_name=OPENSEARCH_TARGET_CONFIG_1.index_name,
            id_field_name=ID_FIELD_NAME
        ))
        load_documents_into_opensearch(
            iter([DOCUMENT_1]),
            client=opensearch_client_mock,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1,
            field_names_for_config=FIELD_NAMES_FOR_CONFIG_1,
            batch_size=BIGQUERY_TO_OPENSEARCH_CONFIG_1.batch_size
        )
        streaming_bulk_mock.assert_called()
        _, kwargs = streaming_bulk_mock.call_args
        assert list(kwargs['actions']) == expected_bulk_actions

    def test_should_consume_streaming_bulk_results(
        self,
        opensearch_client_mock: MagicMock,
        streaming_bulk_mock: MagicMock
    ):
        streaming_bulk_ok: bool = True
        streaming_bulk_item: dict = {'dummy_bulk': DOCUMENT_1}
        streaming_bulk_result_iterable = iter([(streaming_bulk_ok, streaming_bulk_item)])
        streaming_bulk_mock.return_value = streaming_bulk_result_iterable
        load_documents_into_opensearch(
            [DOCUMENT_1],
            client=opensearch_client_mock,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1,
            field_names_for_config=FIELD_NAMES_FOR_CONFIG_1,
            batch_size=BIGQUERY_TO_OPENSEARCH_CONFIG_1.batch_size
        )
        assert not list(streaming_bulk_result_iterable)


class TestCreateOrUpdateIndexAndLoadDocumentsIntoOpenSearch:
    def test_should_pass_config_to_create_or_update_opensearch_index_method(
        self,
        get_opensearch_client_mock: MagicMock,
        create_or_update_opensearch_index_mock: MagicMock
    ):
        create_or_update_index_and_load_documents_into_opensearch(
            [DOCUMENT_1],
            config=BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        create_or_update_opensearch_index_mock.assert_called_with(
            client=get_opensearch_client_mock.return_value,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1
        )

    def test_should_pass_documents_and_config_to_load_documents_method(
        self,
        get_opensearch_client_mock: MagicMock,
        load_documents_into_opensearch_mock: MagicMock
    ):
        create_or_update_index_and_load_documents_into_opensearch(
            [DOCUMENT_1],
            config=BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        load_documents_into_opensearch_mock.assert_called_with(
            [DOCUMENT_1],
            client=get_opensearch_client_mock.return_value,
            opensearch_target_config=OPENSEARCH_TARGET_CONFIG_1,
            field_names_for_config=FIELD_NAMES_FOR_CONFIG_1,
            batch_size=1
        )

    def test_should_batch_documents(
        self,
        load_documents_into_opensearch_mock: MagicMock
    ):
        create_or_update_index_and_load_documents_into_opensearch(
            [DOCUMENT_1, DOCUMENT_2, DOCUMENT_3],
            config=dataclasses.replace(
                BIGQUERY_TO_OPENSEARCH_CONFIG_1,
                batch_size=2
            )
        )
        load_documents_into_opensearch_mock.assert_has_calls([
            call(
                [DOCUMENT_1, DOCUMENT_2],
                client=ANY,
                opensearch_target_config=ANY,
                field_names_for_config=ANY,
                batch_size=2
            ),
            call(
                [DOCUMENT_3],
                client=ANY,
                opensearch_target_config=ANY,
                field_names_for_config=ANY,
                batch_size=1
            )
        ])

    def test_should_update_state(
        self,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        create_or_update_index_and_load_documents_into_opensearch(
            [DOCUMENT_1],
            config=BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        save_state_to_s3_for_config_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1.state,
            DOCUMENT_1[TIMESTAMP_FIELD_NAME]
        )

    def test_should_pass_opensearch_target_config_to_get_cient(
        self,
        get_opensearch_client_mock: MagicMock
    ):
        create_or_update_index_and_load_documents_into_opensearch(
            [DOCUMENT_1],
            config=BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        get_opensearch_client_mock.assert_called_with(OPENSEARCH_TARGET_CONFIG_1)


class TestFetchDocumentsFromBigQueryAndLoadIntoOpenSearch:
    def test_should_fetch_documents_from_bigquery_and_pass_to_opensearch(
        self,
        iter_documents_from_bigquery_mock: MagicMock,
        create_or_update_index_and_load_documents_into_opensearch_mock: MagicMock,
        load_state_or_default_from_s3_for_config_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_load_into_opensearch(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        iter_documents_from_bigquery_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1,
            start_timestamp=load_state_or_default_from_s3_for_config_mock.return_value
        )
        create_or_update_index_and_load_documents_into_opensearch_mock.assert_called_with(
            iter_documents_from_bigquery_mock.return_value,
            config=BIGQUERY_TO_OPENSEARCH_CONFIG_1
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
