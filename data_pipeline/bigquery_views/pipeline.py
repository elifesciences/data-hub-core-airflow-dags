from dataclasses import dataclass
import logging
import os

from google.cloud import bigquery

from bigquery_views_manager.view_list import load_view_list_config
from bigquery_views_manager.materialize_views import (
    materialize_views
)

from data_pipeline.utils.pipeline_file_io import get_temp_local_file_if_remote


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BigQueryViewsConfig:
    bigquery_views_config_path: str
    gcp_project: str
    dataset: str


def load_remote_view_list_config(urlpath: str, **kwargs):
    LOGGER.info('loading view list config: %s', urlpath)
    with get_temp_local_file_if_remote(urlpath) as local_path:
        LOGGER.info('loading local view list config: %s', local_path)
        return load_view_list_config(local_path, **kwargs)


def get_client(config: BigQueryViewsConfig) -> bigquery.Client:
    return bigquery.Client(project=config.gcp_project)


def materialize_bigquery_views(config: BigQueryViewsConfig):
    LOGGER.info('config: %s', config)
    views_config_file_path = os.path.join(
        config.bigquery_views_config_path,
        'views.yml'
    )
    client = get_client(config)
    view_list_config = load_remote_view_list_config(
        str(views_config_file_path)
    ).resolve_conditions({
        'project': client.project,
        'dataset': config.dataset
    })
    LOGGER.info('view_list_config: %s', view_list_config)

    views_ordered_dict_all = view_list_config.to_views_ordered_dict(
        config.dataset
    )
    LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)
    materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
        config.dataset
    )
    LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

    materialize_views(
        client=client,
        materialized_view_dict=materialized_view_ordered_dict_all,
        source_view_dict=views_ordered_dict_all,
        project=client.project
    )
