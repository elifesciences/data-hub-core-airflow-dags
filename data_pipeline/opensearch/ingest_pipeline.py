import json
import logging
from typing import Iterable, Sequence, Tuple

from opensearchpy import OpenSearch

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    OpenSearchIngestPipelineConfig,
    OpenSearchIngestPipelineTestConfig
)


LOGGER = logging.getLogger(__name__)


def get_opensearch_ingest_pipeline_tests_body(
    ingest_pipeline_config: OpenSearchIngestPipelineConfig
) -> dict:
    return {
        'pipeline': json.loads(ingest_pipeline_config.definition),
        'docs': [
            {
                '_source': ingest_pipeline_test_config.input_document
            }
            for ingest_pipeline_test_config in ingest_pipeline_config.tests
        ]
    }


def get_opensearch_ingest_pipeline_test_actual_documents_from_simulate_response(
    simulate_response: dict
) -> Sequence[dict]:
    LOGGER.debug('simulate_response: %r', simulate_response)
    return [
        doc['doc']['_source']
        for doc in simulate_response['docs']
    ]


def iter_failed_opensearch_ingest_pipeline_test_and_actual_document(
    actual_documents: Sequence[dict],
    ingest_pipeline_config: OpenSearchIngestPipelineConfig
) -> Iterable[Tuple[OpenSearchIngestPipelineTestConfig, dict]]:
    assert len(actual_documents) == len(ingest_pipeline_config.tests)
    for ingest_pipeline_test_config, actual_document in zip(
        ingest_pipeline_config.tests,
        actual_documents
    ):
        if actual_document != ingest_pipeline_test_config.expected_document:
            yield ingest_pipeline_test_config, actual_document


def assert_opensearch_ingest_pipeline_test_actual_documents_match_expected(
    actual_documents: Sequence[dict],
    ingest_pipeline_config: OpenSearchIngestPipelineConfig
):
    failed_opensearch_ingest_pipeline_test_and_actual_document = list(
        iter_failed_opensearch_ingest_pipeline_test_and_actual_document(
            actual_documents=actual_documents,
            ingest_pipeline_config=ingest_pipeline_config
        )
    )
    if not failed_opensearch_ingest_pipeline_test_and_actual_document:
        passed_descriptions = [
            ingest_pipeline_test_config.description
            for ingest_pipeline_test_config in ingest_pipeline_config.tests
        ]
        LOGGER.info('Ingest pipeline tests passed: %r', passed_descriptions)
        return
    failed_descriptions = []
    for opensearch_ingest_pipeline_test_config, actual_document in (
        failed_opensearch_ingest_pipeline_test_and_actual_document
    ):
        LOGGER.warning(
            'Ingest pipeline test failed: description=%r, actual=%r, expected=%r',
            opensearch_ingest_pipeline_test_config.description,
            actual_document,
            opensearch_ingest_pipeline_test_config.expected_document
        )
        failed_descriptions.append(opensearch_ingest_pipeline_test_config.description)
    raise AssertionError(f'Ingest pipeline test failures: {failed_descriptions}')


def run_opensearch_ingest_pipeline_tests(
    client: OpenSearch,
    ingest_pipeline_config: OpenSearchIngestPipelineConfig
):
    LOGGER.info('Testing ingest pipeline: %r', ingest_pipeline_config.name)
    simulate_response = client.ingest.simulate(
        body=get_opensearch_ingest_pipeline_tests_body(
            ingest_pipeline_config
        )
    )
    actual_documents = get_opensearch_ingest_pipeline_test_actual_documents_from_simulate_response(
        simulate_response
    )
    assert_opensearch_ingest_pipeline_test_actual_documents_match_expected(
        actual_documents=actual_documents,
        ingest_pipeline_config=ingest_pipeline_config
    )


def create_or_update_opensearch_ingest_pipeline(
    client: OpenSearch,
    ingest_pipeline_config: OpenSearchIngestPipelineConfig
):
    if ingest_pipeline_config.tests:
        run_opensearch_ingest_pipeline_tests(
            client=client,
            ingest_pipeline_config=ingest_pipeline_config
        )
    LOGGER.info('Creating or updating ingest pipeline: %r', ingest_pipeline_config.name)
    client.ingest.put_pipeline(
        id=ingest_pipeline_config.name,
        body=ingest_pipeline_config.definition
    )


def create_or_update_opensearch_ingest_pipelines(
    client: OpenSearch,
    ingest_pipeline_config_list: Sequence[OpenSearchIngestPipelineConfig]
):
    LOGGER.debug('ingest_pipeline_config_list: %r', ingest_pipeline_config_list)
    for ingest_pipeline_config in ingest_pipeline_config_list:
        create_or_update_opensearch_ingest_pipeline(
            client=client,
            ingest_pipeline_config=ingest_pipeline_config
        )
