import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import data_pipeline.bigquery_views.pipeline as target_module
from data_pipeline.bigquery_views.pipeline import (
    BigQueryViewsConfig,
    get_client,
    load_remote_view_mapping,
    materialize_bigquery_views
)


LOGGER = logging.getLogger(__name__)


GCP_PROJECT_1 = 'gcp-project-1'


@pytest.fixture(name='bigquery', autouse=True)
def _bigquery() -> MagicMock:
    with patch.object(target_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='mock_materialize_views', autouse=True)
def _mock_materialize_views() -> MagicMock:
    with patch.object(target_module, 'materialize_views') as mock:
        yield mock


@pytest.fixture(name='mock_get_client', autouse=False)
def _mock_get_client() -> MagicMock:
    with patch.object(target_module, 'get_client') as mock:
        yield mock


@pytest.fixture(name='bigquery_views_config')
def _bigquery_views_config() -> MagicMock:
    config = MagicMock(name='config')
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


class TestLoadRemoteViewMapping:
    def test_can_load_local_view_mapping(self, temp_dir: Path):
        view_mapping_path = temp_dir / 'views.lst'
        view_mapping_path.write_text('\n'.join(['view1', 'view2']))
        view_mapping = load_remote_view_mapping(
            view_mapping_path,
            should_map_table=False,
            default_dataset_name='dataset1'
        )
        LOGGER.debug('view_mapping: %s', view_mapping)
        assert set(view_mapping.keys()) == {
            'view1',
            'view2'
        }


class TestMaterializeBigQueryViews:
    def test_should_call_materialize_views(
            self,
            bigquery_views_config: BigQueryViewsConfig,
            temp_dir: Path,
            mock_materialize_views: MagicMock,
            mock_get_client: MagicMock):
        client = mock_get_client.return_value
        bigquery_views_config.bigquery_views_config_path = str(temp_dir)
        view_mapping_path = temp_dir / 'views.lst'
        view_mapping_path.write_text('\n'.join(['view1', 'view2']))
        materialized_view_mapping_path = temp_dir / 'materialized-views.lst'
        materialized_view_mapping_path.write_text('\n'.join(['view1']))
        materialize_bigquery_views(bigquery_views_config)
        mock_materialize_views.assert_called()
        kwargs = mock_materialize_views.call_args[1]
        assert kwargs['client'] == client
        assert kwargs['project'] == client.project
        assert kwargs['materialized_view_dict'].keys() == {'view1'}
        assert kwargs['source_view_dict'].keys() == {'view1', 'view2'}
