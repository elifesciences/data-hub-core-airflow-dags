import dataclasses
import json
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

import data_pipeline.opensearch.ingest_pipeline as test_module
from data_pipeline.opensearch.ingest_pipeline import (
    assert_opensearch_ingest_pipeline_test_actual_documents_match_expected,
    create_or_update_opensearch_ingest_pipeline,
    get_opensearch_ingest_pipeline_test_actual_documents_from_simulate_response,
    get_opensearch_ingest_pipeline_tests_body,
    run_opensearch_ingest_pipeline_tests
)
from tests.unit_test.opensearch.bigquery_to_opensearch_pipeline_test import (
    OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
    OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1
)


@pytest.fixture(name='opensearch_client_mock')
def _opensearch_client_mock() -> MagicMock:
    return MagicMock(name='opensearch_client_mock')


@pytest.fixture(name='assert_opensearch_ingest_pipeline_test_actual_documents_match_expected_mock')
def _assert_opensearch_ingest_pipeline_test_actual_documents_match_expected_mock(
) -> Iterator[MagicMock]:
    with patch.object(
        test_module,
        'assert_opensearch_ingest_pipeline_test_actual_documents_match_expected'
    ) as mock:
        yield mock


@pytest.fixture(name='run_opensearch_ingest_pipeline_tests_mock')
def _run_opensearch_ingest_pipeline_tests_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'run_opensearch_ingest_pipeline_tests') as mock:
        yield mock


class TestGetOpenSearchIngestPipelineTestsBody:
    def test_should_parse_pipeline_definition_as_json(self):
        body = get_opensearch_ingest_pipeline_tests_body(
            ingest_pipeline_config=dataclasses.replace(
                OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
                tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
            )
        )
        assert body['pipeline'] == json.loads(OPENSEARCH_INGEST_PIPELINE_CONFIG_1.definition)

    def test_should_include_input_documents_as_source(self):
        body = get_opensearch_ingest_pipeline_tests_body(
            ingest_pipeline_config=dataclasses.replace(
                OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
                tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
            )
        )
        assert len(body['docs']) == 1
        assert body['docs'][0]['_source'] == (
            OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1.input_document
        )


class TestGetOpenSearchIngestPipelineTestActualDocumentsFromSimulateResponse:
    def test_should_extract_actual_documents_from_source(self):
        actual_documents = (
            get_opensearch_ingest_pipeline_test_actual_documents_from_simulate_response({
                'docs': [{
                    'doc': {
                        '_source': OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1.expected_document
                    }
                }]
            })
        )
        assert actual_documents == [OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1.expected_document]


class TestAssertOpenSearchIngestPipelineTestActualDocumentsMatchExpected:
    def test_should_pass_if_actual_documents_match_expected_documents(self):
        assert_opensearch_ingest_pipeline_test_actual_documents_match_expected(
            actual_documents=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1.expected_document],
            ingest_pipeline_config=dataclasses.replace(
                OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
                tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
            )
        )

    def test_should_raise_assertion_error_for_not_matching_documents(self):
        with pytest.raises(AssertionError):
            assert_opensearch_ingest_pipeline_test_actual_documents_match_expected(
                actual_documents=[{'name': 'not matching actual document'}],
                ingest_pipeline_config=dataclasses.replace(
                    OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
                    tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
                )
            )


class TestRunOpenSearchIngestPipelineTests:
    def test_should_simulate_and_assert_documents_match(
        self,
        opensearch_client_mock: MagicMock,
        assert_opensearch_ingest_pipeline_test_actual_documents_match_expected_mock: MagicMock
    ):
        ingest_pipeline_config = dataclasses.replace(
            OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
            tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
        )
        opensearch_client_mock.ingest.simulate.return_value = {
            'docs': [{
                'doc': {
                    '_source': {'name': 'actual document 1'}
                }
            }]
        }
        run_opensearch_ingest_pipeline_tests(
            client=opensearch_client_mock,
            ingest_pipeline_config=ingest_pipeline_config
        )
        opensearch_client_mock.ingest.simulate.assert_called_with(
            body=get_opensearch_ingest_pipeline_tests_body(
                ingest_pipeline_config
            )
        )
        (
            assert_opensearch_ingest_pipeline_test_actual_documents_match_expected_mock
            .assert_called_with(
                actual_documents=[{'name': 'actual document 1'}],
                ingest_pipeline_config=ingest_pipeline_config
            )
        )


class TestCreateOrUpdateOpenSearchIngestPipeline:
    def test_should_put_ingest_pipeline(
        self,
        opensearch_client_mock: MagicMock
    ):
        create_or_update_opensearch_ingest_pipeline(
            client=opensearch_client_mock,
            ingest_pipeline_config=OPENSEARCH_INGEST_PIPELINE_CONFIG_1
        )
        opensearch_client_mock.ingest.put_pipeline.assert_called_with(
            id=OPENSEARCH_INGEST_PIPELINE_CONFIG_1.name,
            body=OPENSEARCH_INGEST_PIPELINE_CONFIG_1.definition
        )

    def test_should_run_ingest_pipeline_tests_if_defined(
        self,
        opensearch_client_mock: MagicMock,
        run_opensearch_ingest_pipeline_tests_mock: MagicMock
    ):
        ingest_pipeline_config = dataclasses.replace(
            OPENSEARCH_INGEST_PIPELINE_CONFIG_1,
            tests=[OPENSEARCH_INGEST_PIPELINE_TEST_CONFIG_1]
        )
        create_or_update_opensearch_ingest_pipeline(
            client=opensearch_client_mock,
            ingest_pipeline_config=ingest_pipeline_config
        )
        run_opensearch_ingest_pipeline_tests_mock.assert_called_with(
            client=opensearch_client_mock,
            ingest_pipeline_config=ingest_pipeline_config
        )
