# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import functools
import os
import logging
from datetime import timedelta
from typing import Optional, Sequence

import airflow
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import DEFAULT_QUEUE

from data_pipeline.s3_csv_data.s3_csv_config import (
    DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE,
    MultiS3CsvConfig,
    S3BaseCsvConfig
)
from data_pipeline.s3_csv_data.s3_csv_config_typing import S3CsvConfigDict
from data_pipeline.s3_csv_data.s3_csv_etl import (
    transform_load_data,
    get_stored_state,
    update_object_latest_dates,
    upload_s3_object_json,
    NamedLiterals
)
from data_pipeline.utils.dags.airflow_s3_util_extension import (
    S3HookNewFileMonitor
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string
)
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)

INITIAL_S3_FILE_LAST_MODIFIED_DATE_ENV_NAME = (
    "INITIAL_S3_FILE_LAST_MODIFIED_DATE"
)

S3_CSV_SCHEDULE_INTERVAL_ENV_NAME = (
    "S3_CSV_SCHEDULE_INTERVAL"
)
S3_CSV_CONFIG_FILE_PATH_ENV_NAME = (
    "S3_CSV_CONFIG_FILE_PATH"
)

S3_CSV_QUEUE_ENV_NAME = (
    "S3_CSV_QUEUE"
)


def get_multi_csv_pipeline_config() -> MultiS3CsvConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        S3_CSV_CONFIG_FILE_PATH_ENV_NAME,
        MultiS3CsvConfig
    )


def etl_new_csv_files(data_config: S3BaseCsvConfig):
    obj_pattern_with_latest_dates = (
        get_stored_state(data_config,
                         get_default_initial_s3_last_modified_date()
                         )
    )
    hook = S3HookNewFileMonitor(
        aws_conn_id=NamedLiterals.DEFAULT_AWS_CONN_ID,
        verify=None
    )
    new_s3_files = hook.get_new_object_key_names(
        obj_pattern_with_latest_dates,
        data_config.s3_bucket_name
    )
    if not new_s3_files:
        LOGGER.info('No new file found and skipped the task.')
        raise AirflowSkipException
    for object_key_pattern, matching_files_list in new_s3_files.items():
        record_import_timestamp_as_string = get_current_timestamp_as_string()
        sorted_matching_files_list = (
            sorted(matching_files_list,
                   key=lambda file_meta:
                   file_meta[NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY]
                   )
        )

        for matching_file_metadata in sorted_matching_files_list:
            transform_load_data(
                matching_file_metadata.get(
                    NamedLiterals.S3_FILE_METADATA_NAME_KEY
                ),
                data_config,
                record_import_timestamp_as_string,
            )
            updated_obj_pattern_with_latest_dates = (
                update_object_latest_dates(
                    obj_pattern_with_latest_dates,
                    object_key_pattern,
                    matching_file_metadata.get(
                        NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY
                    )
                )
            )
            upload_s3_object_json(
                updated_obj_pattern_with_latest_dates,
                data_config.state_file_bucket_name,
                data_config.state_file_object_name
            )


def csv_etl(data_pipeline_id: str, **_kwargs):
    multi_csv_pipeline_config = get_multi_csv_pipeline_config()
    data_config_dict = multi_csv_pipeline_config.s3_csv_config_dict_by_pipeline_id[data_pipeline_id]
    data_config = S3BaseCsvConfig(data_config_dict)
    etl_new_csv_files(data_config=data_config)


def get_dag_id_for_s3_csv_config_dict(s3_csv_config_dict: S3CsvConfigDict) -> str:
    return f'CSV.{s3_csv_config_dict["dataPipelineId"]}'


def get_queue() -> str:
    return os.getenv(
        S3_CSV_QUEUE_ENV_NAME,
        DEFAULT_QUEUE
    )


def create_csv_pipeline_dags(
    default_schedule: Optional[str] = None
) -> Sequence[airflow.DAG]:
    dags = []
    multi_csv_pipeline_config = get_multi_csv_pipeline_config()
    for data_pipeline_id, s3_csv_config_dict in (
        multi_csv_pipeline_config.s3_csv_config_dict_by_pipeline_id.items()
    ):
        airflow_config = AirflowConfig.from_optional_dict(
            s3_csv_config_dict.get('airflow')
        )
        with create_dag(
            dag_id=get_dag_id_for_s3_csv_config_dict(s3_csv_config_dict),
            description=s3_csv_config_dict.get('description'),
            schedule=default_schedule,
            dagrun_timeout=timedelta(days=1)
        ) as dag:
            create_python_task(
                dag=dag,
                task_id="csv_etl",
                python_callable=functools.partial(
                    csv_etl,
                    data_pipeline_id=data_pipeline_id
                ),
                queue=get_queue(),
                **airflow_config.task_parameters
            )
            dags.append(dag)
    return dags


def get_default_initial_s3_last_modified_date():
    return os.getenv(
        INITIAL_S3_FILE_LAST_MODIFIED_DATE_ENV_NAME,
        DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
    )


def get_default_schedule() -> Optional[str]:
    return get_environment_variable_value(
        S3_CSV_SCHEDULE_INTERVAL_ENV_NAME,
        default_value=None
    )


DAGS = create_csv_pipeline_dags(default_schedule=get_default_schedule())

FIRST_DAG = DAGS[0]
