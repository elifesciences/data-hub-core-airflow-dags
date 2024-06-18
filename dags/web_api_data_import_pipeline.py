# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import functools
import logging
from datetime import timedelta
from typing import Optional, Sequence

import airflow

from data_pipeline.generic_web_api.generic_web_api_config import (
    MultiWebApiConfig,
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict
from data_pipeline.generic_web_api.generic_web_api_data_etl import (
    generic_web_api_data_etl
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task,
)
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


WEB_API_SCHEDULE_INTERVAL_ENV_NAME = (
    "WEB_API_SCHEDULE_INTERVAL"
)

WEB_API_CONFIG_FILE_PATH_ENV_NAME = (
    "WEB_API_CONFIG_FILE_PATH"
)


def get_multi_web_api_config() -> MultiWebApiConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        WEB_API_CONFIG_FILE_PATH_ENV_NAME,
        MultiWebApiConfig
    )


def get_default_schedule() -> Optional[str]:
    return get_environment_variable_value(
        WEB_API_SCHEDULE_INTERVAL_ENV_NAME,
        default_value=None
    )


def web_api_data_etl(data_pipeline_id: str, **_kwargs):
    multi_web_api_config = get_multi_web_api_config()
    data_config_dict = multi_web_api_config.web_api_config_dict_by_pipeline_id[data_pipeline_id]
    data_config = WebApiConfig.from_dict(data_config_dict)
    generic_web_api_data_etl(data_config=data_config)


def get_dag_id_for_web_api_config_dict(web_api_config_dict: WebApiConfigDict) -> str:
    return f'Web_API.{web_api_config_dict["dataPipelineId"]}'


def create_web_api_dags(
    default_schedule: Optional[str] = None
) -> Sequence[airflow.DAG]:
    dags = []
    multi_web_api_config = get_multi_web_api_config()
    default_airflow_config = AirflowConfig(
        dag_parameters={
            'schedule': default_schedule,
            'dagrun_timeout': timedelta(days=1),
        }
    )
    for data_pipeline_id, web_api_config_dict in (
        multi_web_api_config.web_api_config_dict_by_pipeline_id.items()
    ):
        airflow_config = AirflowConfig.from_optional_dict(
            web_api_config_dict.get('airflow'),
            default_airflow_config=default_airflow_config
        )
        with create_dag(
            dag_id=get_dag_id_for_web_api_config_dict(web_api_config_dict),
            description=web_api_config_dict.get('description'),
            **airflow_config.dag_parameters
        ) as dag:
            create_python_task(
                dag=dag,
                task_id="web_api_data_etl",
                python_callable=functools.partial(
                    web_api_data_etl,
                    data_pipeline_id=data_pipeline_id
                ),
                **airflow_config.task_parameters
            )
            dags.append(dag)
    return dags


DAGS = create_web_api_dags(default_schedule=get_default_schedule())

FIRST_DAG = DAGS[0]
