from dags.materialize_bigquery_views_pipeline import (
    EnvironmentVariables,
    get_config
)


GCP_PROJECT_1 = 'gcp-project-1'
DATASET_1 = 'dataset1'


class TestGetConfig:
    def test_should_get_config_from_env_vars(self, mock_env: dict):
        mock_env[EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH] = (
            '/path/to/config'
        )
        mock_env[EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT] = (
            GCP_PROJECT_1
        )
        mock_env[EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_DATASET] = (
            DATASET_1
        )
        mock_env[EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_VIEW_MAPPING_ENABLED] = (
            'false'
        )
        config = get_config()
        assert config.bigquery_views_config_path == '/path/to/config'
        assert config.gcp_project == GCP_PROJECT_1
        assert config.dataset == DATASET_1
        assert not config.view_name_mapping_enabled
