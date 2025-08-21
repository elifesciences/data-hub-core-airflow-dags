# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from datetime import timedelta
import functools
import os
import logging
from typing import Sequence

import airflow

from data_pipeline.s3_csv_data.cli import etl_new_csv_files
from data_pipeline.s3_csv_data.s3_csv_config import (
    DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE,
    MultiS3CsvConfig,
    S3BaseCsvConfig
)
from data_pipeline.s3_csv_data.s3_csv_config_typing import S3CsvConfigDict
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)

INITIAL_S3_FILE_LAST_MODIFIED_DATE_ENV_NAME = (
    "INITIAL_S3_FILE_LAST_MODIFIED_DATE"
)

S3_CSV_CONFIG_FILE_PATH_ENV_NAME = (
    "S3_CSV_CONFIG_FILE_PATH"
)


def get_multi_csv_pipeline_config() -> MultiS3CsvConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        S3_CSV_CONFIG_FILE_PATH_ENV_NAME,
        MultiS3CsvConfig
    )


def csv_etl(data_pipeline_id: str, **_kwargs):
    multi_csv_pipeline_config = get_multi_csv_pipeline_config()
    data_config_dict = multi_csv_pipeline_config.s3_csv_config_dict_by_pipeline_id[data_pipeline_id]
    data_config = S3BaseCsvConfig(data_config_dict)
    etl_new_csv_files(data_config=data_config)


def get_dag_id_for_s3_csv_config_dict(s3_csv_config_dict: S3CsvConfigDict) -> str:
    return f'CSV.{s3_csv_config_dict["dataPipelineId"]}'


def create_csv_pipeline_dags() -> Sequence[airflow.DAG]:
    dags = []
    multi_csv_pipeline_config = get_multi_csv_pipeline_config()
    default_airflow_config = multi_csv_pipeline_config.default_airflow_config
    for data_pipeline_id, s3_csv_config_dict in (
        multi_csv_pipeline_config.s3_csv_config_dict_by_pipeline_id.items()
    ):
        airflow_config = AirflowConfig.from_optional_dict(
            s3_csv_config_dict.get('airflow'),
            default_airflow_config=default_airflow_config
        )
        with create_dag(
            dag_id=get_dag_id_for_s3_csv_config_dict(s3_csv_config_dict),
            description=s3_csv_config_dict.get('description'),
            dagrun_timeout=timedelta(days=1),
            **airflow_config.dag_parameters
        ) as dag:
            create_python_task(
                dag=dag,
                task_id="csv_etl",
                python_callable=functools.partial(
                    csv_etl,
                    data_pipeline_id=data_pipeline_id
                ),
                **airflow_config.task_parameters
            )
            dags.append(dag)
    return dags


def get_default_initial_s3_last_modified_date():
    return os.getenv(
        INITIAL_S3_FILE_LAST_MODIFIED_DATE_ENV_NAME,
        DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
    )


DAGS = create_csv_pipeline_dags()

FIRST_DAG = DAGS[0]
