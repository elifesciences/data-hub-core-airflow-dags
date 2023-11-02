import dataclasses
from datetime import datetime
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock
from typing import Iterable

import pytest

from bigquery_views_manager.materialize_views import (
    MaterializeViewListResult,
    MaterializeViewResult
)

from bigquery_views_manager.view_list import (
    DATASET_NAME_KEY,
    VIEW_OR_TABLE_NAME_KEY
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

MATCHING_DATASET_1 = 'matching_dataset1'
OTHER_DATASET_1 = 'other_dataset1'
OUTPUT_DATASET_1 = 'output_dataset1'
OUTPUT_TABLE_1 = 'output_table1'

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


@pytest.fixture(name='get_current_timestamp_mock', autouse=True)
def _get_current_timestamp_mock() -> Iterable[MagicMock]:
    with patch.object(target_module, 'get_current_timestamp') as mock:
        mock.return_value = CURRENT_TIMESTAMP
        yield mock


@pytest.fixture(name='bigquery', autouse=True)
def _bigquery() -> Iterable[MagicMock]:
    with patch.object(target_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='mock_materialize_views', autouse=True)
def _mock_materialize_views() -> Iterable[MagicMock]:
    with patch.object(target_module, 'materialize_views') as mock:
        mock.return_value = MaterializeViewListResult(result_list=[])
        yield mock


@pytest.fixture(name='mock_load_view_list_config', autouse=False)
def _mock_load_view_list_config() -> Iterable[MagicMock]:
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


@pytest.fixture(name='sample_config_path')
def _views_sample_config_path(tmp_path: Path) -> Path:
    sample_config_path = tmp_path / 'sample_config'
    sample_config_path.mkdir()
    view_list_config_path = sample_config_path / 'views.yml'
    view_list_config_path.write_text('\n'.join([
        '- view1:',
        '    materialize: true',
        '    conditions:',
        '    - if:',
        f'        dataset: {MATCHING_DATASET_1}',
        f'      materialize_as: {OUTPUT_DATASET_1}.{OUTPUT_TABLE_1}',
        '- view2'
    ]))
    return sample_config_path


@pytest.fixture(name='bigquery_views_config')
def _bigquery_views_config(sample_config_path: Path) -> BigQueryViewsConfig:
    config = BigQueryViewsConfig(
        bigquery_views_config_path=str(sample_config_path),
        gcp_project=GCP_PROJECT_1,
        dataset=OTHER_DATASET_1
    )
    return config


class TestGetClient:
    def test_should_pass_gcp_project(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            bigquery: MagicMock):
        get_client(bigquery_views_config)
        bigquery.Client.assert_called_with(
            project=bigquery_views_config.gcp_project
        )


class TestLoadRemoteViewListConfig:
    def test_can_load_local_view_list_config(self, tmp_path: Path):
        view_list_config_path = tmp_path / 'views.yml'
        view_list_config_path.write_text('\n'.join([
            '- view1',
            '- view2'
        ]))
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
    def test_should_call_materialize_views(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            mock_materialize_views: MagicMock,
            mock_get_client: MagicMock):
        client = mock_get_client.return_value
        materialize_bigquery_views(bigquery_views_config)
        mock_materialize_views.assert_called()
        kwargs = mock_materialize_views.call_args[1]
        assert kwargs['client'] == client
        assert kwargs['project'] == client.project
        assert kwargs['materialized_view_dict'].keys() == {'view1'}
        assert kwargs['source_view_dict'].keys() == {'view1', 'view2'}

    def test_should_call_map_output_dataset_table_if_matching_default_dataset(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            mock_materialize_views: MagicMock):
        LOGGER.info(bigquery_views_config)
        materialize_bigquery_views(
            dataclasses.replace(bigquery_views_config, dataset=MATCHING_DATASET_1)
        )
        mock_materialize_views.assert_called()
        kwargs = mock_materialize_views.call_args[1]
        assert kwargs['materialized_view_dict'] == {
            'view1': {
                DATASET_NAME_KEY: OUTPUT_DATASET_1,
                VIEW_OR_TABLE_NAME_KEY: OUTPUT_TABLE_1
            }
        }

    def test_should_call_not_map_output_dataset_table_if_not_matching_default_dataset(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            mock_materialize_views: MagicMock):
        materialize_bigquery_views(
            dataclasses.replace(bigquery_views_config, dataset=OTHER_DATASET_1)
        )
        mock_materialize_views.assert_called()
        kwargs = mock_materialize_views.call_args[1]
        assert kwargs['materialized_view_dict'] == {
            'view1': {
                DATASET_NAME_KEY: OTHER_DATASET_1,
                VIEW_OR_TABLE_NAME_KEY: 'mview1'
            }
        }

    def test_should_call_load_given_json_list_data_from_tempdir_to_bq_func(
        self,
        bigquery_views_config: BigQueryViewsConfig,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock,
        mock_materialize_views: MagicMock
    ):
        mock_materialize_views.return_value = MaterializeViewListResult(
            result_list=[MATERIALIZE_VIEW_RESULT_1]
        )
        json_list = get_json_list_for_materialize_views_log(mock_materialize_views.return_value)
        materialize_bigquery_views(bigquery_views_config)
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=bigquery_views_config.gcp_project,
            dataset_name=bigquery_views_config.dataset,
            table_name=bigquery_views_config.log_table_name,
            json_list=json_list
        )
