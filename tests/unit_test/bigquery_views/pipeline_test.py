import dataclasses
from datetime import datetime
import logging
from pathlib import Path
from unittest.mock import ANY, patch, MagicMock
from typing import Iterable

import pytest

from bigquery_views_manager.view_list import (
    ViewConfig,
    ViewListConfig,
    load_view_list_config
)
from bigquery_views_manager.materialize_views import (
    MaterializeViewListResult,
    MaterializeViewResult
)

import data_pipeline.bigquery_views.pipeline as target_module
from data_pipeline.bigquery_views.pipeline import (
    BigQueryViewsConfig,
    get_client,
    get_json_list_for_materialize_views_log,
    load_remote_view_list_config,
    materialize_bigquery_views
)


LOGGER = logging.getLogger(__name__)


GCP_PROJECT_1 = 'gcp-project-1'

CURRENT_TIMESTAMP = datetime.fromisoformat('2023-01-02T03:04:05+00:00')

OTHER_DATASET_1 = 'other_dataset1'

VIEW_LIST_CONFIG_1 = ViewListConfig([
    ViewConfig(view_name='view_1'),
    ViewConfig(view_name='view_2', materialize=True)
])

MATERIALIZE_VIEW_RESULT_1 = MaterializeViewResult(
    source_dataset='source_dataset_1',
    source_view_name='source_view_name_1',
    destination_dataset='destination_dataset_1',
    destination_table_name='destination_table_name_1',
    total_bytes_processed=10,
    total_rows=10,
    duration=3,
    cache_hit=10,
    slot_millis=10,
    total_bytes_billed=10
)


BIGQUERY_VIEWS_CONFIG_1 = BigQueryViewsConfig(
    bigquery_views_config_path='/dummy/views-config',
    gcp_project=GCP_PROJECT_1,
    dataset=OTHER_DATASET_1
)


@pytest.fixture(name='get_current_timestamp_mock', autouse=True)
def _get_current_timestamp_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'get_current_timestamp') as mock:
        mock.return_value = CURRENT_TIMESTAMP
        yield mock


@pytest.fixture(name='bigquery', autouse=True)
def _bigquery() -> Iterable[MagicMock]:
    with patch.object(target_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='materialize_views_if_necessary_mock', autouse=True)
def _materialize_views_if_necessary_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'materialize_views_if_necessary') as mock:
        mock.return_value = MaterializeViewListResult(result_list=[])
        yield mock


@pytest.fixture(name='load_view_list_config_mock', autouse=True)
def _load_view_list_config_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'load_view_list_config') as mock:
        yield mock


@pytest.fixture(name='mock_get_client', autouse=False)
def _mock_get_client() -> Iterable[MagicMock]:
    with patch.object(target_module, 'get_client') as mock:
        yield mock


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        target_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


class TestGetClient:
    def test_should_pass_gcp_project(
        self,
        bigquery: MagicMock
    ):
        get_client(BIGQUERY_VIEWS_CONFIG_1)
        bigquery.Client.assert_called_with(
            project=BIGQUERY_VIEWS_CONFIG_1.gcp_project
        )


class TestLoadRemoteViewListConfig:
    def test_can_load_local_view_list_config(
        self,
        tmp_path: Path,
        load_view_list_config_mock: MagicMock
    ):
        view_list_config_path = tmp_path / 'views.yml'
        view_list_config_path.write_text('\n'.join([
            '- view1',
            '- view2'
        ]))
        load_view_list_config_mock.side_effect = load_view_list_config
        view_list_config = load_remote_view_list_config(
            str(view_list_config_path)
        )
        LOGGER.debug('view_list_config: %s', view_list_config)
        assert set(view_list_config.view_names) == {
            'view1',
            'view2'
        }


class TestGetJsonListForMaterializeViewsLog:
    def test_should_return_json_list_from_materialize_view_list_result(self):
        result = get_json_list_for_materialize_views_log(
            MaterializeViewListResult(
                result_list=[MATERIALIZE_VIEW_RESULT_1]
            )
        )
        assert result == [{
            'data_hub_imported_timestamp': CURRENT_TIMESTAMP.isoformat(),
            'source_dataset': 'source_dataset_1',
            'source_view_name': 'source_view_name_1',
            'destination_dataset': 'destination_dataset_1',
            'destination_table_name': 'destination_table_name_1',
            'total_bytes_processed': 10,
            'total_rows': 10,
            'duration': 3,
            'cache_hit': 10,
            'slot_millis': 10,
            'total_bytes_billed': 10
        }]

    def test_should_remove_fields_with_none_value(self):
        result = get_json_list_for_materialize_views_log(
            MaterializeViewListResult(
                result_list=[
                    dataclasses.replace(MATERIALIZE_VIEW_RESULT_1, total_bytes_billed=None)
                ]
            )
        )
        assert 'total_bytes_billed' not in result[0]


class TestMaterializeBigQueryViews:
    def test_should_call_materialize_views_if_necessary(
        self,
        materialize_views_if_necessary_mock: MagicMock,
        load_view_list_config_mock: MagicMock,
        mock_get_client: MagicMock
    ):
        load_view_list_config_mock.return_value = VIEW_LIST_CONFIG_1
        client = mock_get_client.return_value
        materialize_bigquery_views(BIGQUERY_VIEWS_CONFIG_1)
        materialize_views_if_necessary_mock.assert_called_with(
            client=client,
            project=client.project,
            dataset=BIGQUERY_VIEWS_CONFIG_1.dataset,
            view_list_config=VIEW_LIST_CONFIG_1
        )

    def test_should_resolve_conditions(
        self,
        materialize_views_if_necessary_mock: MagicMock,
        load_view_list_config_mock: MagicMock
    ):
        view_list_config_mock = MagicMock(name='view_list_config_mock')
        load_view_list_config_mock.return_value = view_list_config_mock
        view_list_config_mock.resolve_conditions.return_value = VIEW_LIST_CONFIG_1
        materialize_bigquery_views(BIGQUERY_VIEWS_CONFIG_1)
        materialize_views_if_necessary_mock.assert_called_with(
            client=ANY,
            project=ANY,
            dataset=ANY,
            view_list_config=VIEW_LIST_CONFIG_1
        )

    def test_should_call_load_given_json_list_data_from_tempdir_to_bq_func(
        self,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock,
        materialize_views_if_necessary_mock: MagicMock
    ):
        materialize_views_if_necessary_mock.return_value = MaterializeViewListResult(
            result_list=[MATERIALIZE_VIEW_RESULT_1]
        )
        json_list = get_json_list_for_materialize_views_log(
            materialize_views_if_necessary_mock.return_value
        )
        materialize_bigquery_views(BIGQUERY_VIEWS_CONFIG_1)
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=BIGQUERY_VIEWS_CONFIG_1.gcp_project,
            dataset_name=BIGQUERY_VIEWS_CONFIG_1.dataset,
            table_name=BIGQUERY_VIEWS_CONFIG_1.log_table_name,
            json_list=json_list
        )
