import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from bigquery_views_manager.view_list import (
    DATASET_NAME_KEY,
    VIEW_OR_TABLE_NAME_KEY
)

import data_pipeline.bigquery_views.pipeline as target_module
from data_pipeline.bigquery_views.pipeline import (
    BigQueryViewsConfig,
    get_client,
    load_remote_view_list_config,
    materialize_bigquery_views
)


LOGGER = logging.getLogger(__name__)


GCP_PROJECT_1 = 'gcp-project-1'

MATCHING_DATASET_1 = 'matching_dataset1'
OTHER_DATASET_1 = 'other_dataset1'
OUTPUT_DATASET_1 = 'output_dataset1'
OUTPUT_TABLE_1 = 'output_table1'


@pytest.fixture(name='bigquery', autouse=True)
def _bigquery() -> MagicMock:
    with patch.object(target_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='mock_materialize_views', autouse=True)
def _mock_materialize_views() -> MagicMock:
    with patch.object(target_module, 'materialize_views') as mock:
        yield mock


@pytest.fixture(name='mock_load_view_list_config', autouse=False)
def _mock_load_view_list_config() -> MagicMock:
    with patch.object(target_module, 'load_view_list_config') as mock:
        yield mock


@pytest.fixture(name='mock_get_client', autouse=False)
def _mock_get_client() -> MagicMock:
    with patch.object(target_module, 'get_client') as mock:
        yield mock


@pytest.fixture(name='sample_config_path')
def _views_sample_config_path(temp_dir: Path) -> Path:
    sample_config_path = temp_dir / 'sample_config'
    sample_config_path.mkdir()
    view_list_config_path = sample_config_path / 'views.yml'
    view_list_config_path.write_text('\n'.join([
        '- view1:',
        '    materialize: true',
        '    conditions:',
        '    - if:',
        '        dataset: %s' % MATCHING_DATASET_1,
        '      materialize_as: %s.%s' % (OUTPUT_DATASET_1, OUTPUT_TABLE_1),
        '- view2'
    ]))
    return sample_config_path


@pytest.fixture(name='bigquery_views_config')
def _bigquery_views_config(sample_config_path: Path) -> MagicMock:
    config = MagicMock(name='config')
    config.bigquery_views_config_path = str(sample_config_path)
    config.gcp_project = GCP_PROJECT_1
    return config


class TestGetClient:
    def test_should_pass_gcp_project(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            bigquery: MagicMock):
        bigquery_views_config.gcp_project = 'test project'
        get_client(bigquery_views_config)
        bigquery.Client.assert_called_with(project='test project')


class TestLoadRemoteViewListConfig:
    def test_can_load_local_view_list_config(self, temp_dir: Path):
        view_list_config_path = temp_dir / 'views.yml'
        view_list_config_path.write_text('\n'.join([
            '- view1',
            '- view2'
        ]))
        view_list_config = load_remote_view_list_config(
            view_list_config_path
        )
        LOGGER.debug('view_list_config: %s', view_list_config)
        assert set(view_list_config.view_names) == {
            'view1',
            'view2'
        }


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
        bigquery_views_config.dataset = MATCHING_DATASET_1
        materialize_bigquery_views(bigquery_views_config)
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
        bigquery_views_config.dataset = OTHER_DATASET_1
        materialize_bigquery_views(bigquery_views_config)
        mock_materialize_views.assert_called()
        kwargs = mock_materialize_views.call_args[1]
        assert kwargs['materialized_view_dict'] == {
            'view1': {
                DATASET_NAME_KEY: OTHER_DATASET_1,
                VIEW_OR_TABLE_NAME_KEY: 'mview1'
            }
        }
