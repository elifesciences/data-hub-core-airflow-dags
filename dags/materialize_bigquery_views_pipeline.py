# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value
)

from data_pipeline.bigquery_views.pipeline import (
    BigQueryViewsConfig,
    materialize_bigquery_views
)


class EnvironmentVariables:
    MATERIALIZE_BIGQUERY_VIEWS_SCHEDULE_INTERVAL = 'MATERIALIZE_BIGQUERY_VIEWS_SCHEDULE_INTERVAL'
    MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH = 'MATERIALIZE_BIGQUERY_VIEWS_CONFIG_PATH'
    MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT = 'MATERIALIZE_BIGQUERY_VIEWS_GCP_PROJECT'
    MATERIALIZE_BIGQUERY_VIEWS_DATASET = 'MATERIALIZE_BIGQUERY_VIEWS_DATASET'


DAG_ID = 'Materialize_BigQuery_Views_Pipeline'


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


def materialize_views_task(**_):
    config = get_config()
    materialize_bigquery_views(config)


MATERIALIZE_BIGQUERY_VIEWS_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=get_environment_variable_value(
        EnvironmentVariables.MATERIALIZE_BIGQUERY_VIEWS_SCHEDULE_INTERVAL,
        default_value=None
    )
)


MATERIALIZE_BIGQUERY_VIEWS_TASK = create_python_task(
    MATERIALIZE_BIGQUERY_VIEWS_DAG,
    'materialize_views',
    materialize_views_task
)
