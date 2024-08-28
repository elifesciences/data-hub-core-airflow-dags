import logging

from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value
)
from data_pipeline.bigquery_views.pipeline import (
    BigQueryViewsConfig,
    materialize_bigquery_views
)

LOGGER = logging.getLogger(__name__)


class EnvironmentVariables:
    MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH = 'MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH'
    MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT = 'MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT'
    MATERIALIZE_BIGQUERY_VIEWS_DATASET = 'MATERIALIZE_BIGQUERY_VIEWS_DATASET'


def get_config() -> BigQueryViewsConfig:
    return BigQueryViewsConfig(
        bigquery_views_config_path=get_environment_variable_value(
            EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH,
            required=True
        ),
        gcp_project=get_environment_variable_value(
            EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT,
            required=True
        ),
        dataset=get_environment_variable_value(
            EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_DATASET,
            required=True
        )
    )


def main():
    config = get_config()
    materialize_bigquery_views(config)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
