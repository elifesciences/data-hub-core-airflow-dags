import airflow

from dags.web_api_data_import_pipeline import (
    WEB_API_CONFIG_FILE_PATH_ENV_NAME,
    get_dag_id_for_web_api_config_dict
)
from data_pipeline.generic_web_api.generic_web_api_config import MultiWebApiConfig
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.utils.pipeline_config import get_pipeline_config_for_env_name_and_config_parser
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def get_test_web_api_config_dict() -> WebApiConfigDict:
    multi_data_config = get_pipeline_config_for_env_name_and_config_parser(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME,
        MultiWebApiConfig
    )
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
