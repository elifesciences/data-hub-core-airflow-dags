import logging
import os

from google.cloud import bigquery

from bigquery_views_manager.view_list import load_view_mapping
from bigquery_views_manager.materialize_views import (
    materialize_views
)

from data_pipeline.utils.pipeline_file_io import get_temp_local_file_if_remote


LOGGER = logging.getLogger(__name__)


class BigQueryViewsConfig:
    def __init__(
            self,
            bigquery_views_config_path: str,
            gcp_project: str,
            dataset: str,
            view_name_mapping_enabled: bool):
        self.bigquery_views_config_path = bigquery_views_config_path
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.view_name_mapping_enabled = view_name_mapping_enabled

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)


def load_remote_view_mapping(urlpath: str, **kwargs):
    LOGGER.info('loading view mapping: %s', urlpath)
    with get_temp_local_file_if_remote(urlpath) as local_path:
        return load_view_mapping(local_path, **kwargs)


def get_client(config: BigQueryViewsConfig) -> bigquery.Client:
    return bigquery.Client(project=config.gcp_project)


def materialize_bigquery_views(config: BigQueryViewsConfig):
    LOGGER.info('config: %s', config)
    disable_view_name_mapping = True
    view_name_mapping_enabled = not disable_view_name_mapping
    materialized_views_list_file_path = os.path.join(
        config.bigquery_views_config_path,
        'materialized-views.lst'
    )
    views_list_file_path = os.path.join(
        config.bigquery_views_config_path,
        'views.lst'
    )
    materialized_view_ordered_dict = load_remote_view_mapping(
        materialized_views_list_file_path,
        should_map_table=view_name_mapping_enabled,
        default_dataset_name=config.dataset,
        is_materialized_view=True,
    )
    views_dict_all = load_view_mapping(
        views_list_file_path,
        should_map_table=view_name_mapping_enabled,
        default_dataset_name=config.dataset,
    )
    client = get_client(config)
    materialize_views(
        client,
        materialized_view_dict=materialized_view_ordered_dict,
        source_view_dict=views_dict_all,
        project=client.project
    )
