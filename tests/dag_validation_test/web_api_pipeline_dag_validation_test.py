import os

import airflow

from dags.web_api_data_import_pipeline import (
    WEB_API_CONFIG_FILE_PATH_ENV_NAME,
    get_dag_id_for_web_api_config_dict
)
from data_pipeline.generic_web_api.generic_web_api_config import MultiWebApiConfig
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def get_test_web_api_config_dict() -> WebApiConfigDict:
    conf_file_path = os.getenv(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    multi_data_config = MultiWebApiConfig(data_config_dict)
    return list(
        multi_data_config.web_api_config.values()
    )[0]


def test_dag_should_contain_named_tasks(dagbag: airflow.models.dagbag.DagBag):
    web_api_config_dict = get_test_web_api_config_dict()
    dag_should_contain_named_tasks(
        dagbag,
        dag_id=get_dag_id_for_web_api_config_dict(web_api_config_dict),
        task_list=['web_api_data_etl']
    )
