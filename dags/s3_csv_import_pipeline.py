import json
import os
from datetime import timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import ShortCircuitOperator

from data_pipeline.s3_csv_data.s3_csv_config import S3CsvConfig
from data_pipeline.s3_csv_data.s3_csv_etl import (
    current_timestamp_as_string,
    transform_load_data,
    get_stored_state,
    update_object_latest_dates,
)
from data_pipeline.utils.dags.airflow_s3_util_extension import (
    S3NewKeySensor,
    S3HookNewFileMonitor,
    DEFAULT_AWS_CONN_ID
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"

DAG_ID = "S3_CSV_Data_Pipeline"
S3_CSV_ETL_DAG = DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=get_default_args(),
    dagrun_timeout=timedelta(minutes=60),
)


class NamedLiterals:
    DAG_RUN = 'dag_run'
    RUN_ID = 'run_id'
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    S3_FILE_METADATA_LAST_MODIFIED_KEY = "LastModified"


def get_env_var_or_use_default(env_var_name, default_value):
    return os.getenv(env_var_name, default_value)


def update_prev_run_id_var_val(**context):
    dag_context = context["ti"]
    data_config = dag_context.xcom_pull(
        key="data_config",
        task_ids="Should_Remaining_Tasks_Execute"
    )
    run_id = context.get(NamedLiterals.RUN_ID)
    Variable.set(
        data_config.etl_id,
        json.dumps({NamedLiterals.RUN_ID: run_id})
    )


def is_dag_etl_running(**context):
    dep_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    data_config = S3CsvConfig(context[NamedLiterals.DAG_RUN].conf, dep_env)
    dag_run_var_value = Variable.get(
        data_config.etl_id, None
    )
    if dag_run_var_value:
        dag_run_var_value_dict = json.loads(dag_run_var_value)
        prev_run_id = dag_run_var_value_dict.get(NamedLiterals.RUN_ID)
        dag_run = DagRun.find(dag_id=DAG_ID, run_id=prev_run_id)[0]
        run_status = dag_run.get_state()
        if run_status == NamedLiterals.DAG_RUNNING_STATUS:
            return False

    context["ti"].xcom_push(key="data_config",
                            value=data_config)
    return True


def etl_new_csv_files(**context):
    dag_context = context["ti"]
    data_config = dag_context.xcom_pull(
        key="data_config",
        task_ids="Should_Remaining_Tasks_Execute"
    )
    obj_pattern_with_latest_dates = (
        get_stored_state(data_config)
    )
    hook = S3HookNewFileMonitor(aws_conn_id=DEFAULT_AWS_CONN_ID, verify=None)
    new_s3_files = hook.get_new_object_key_names(
        obj_pattern_with_latest_dates,
        data_config.s3_bucket_name
    )
    for object_key_pattern, matching_files_list in new_s3_files.items():
        record_import_timestamp_as_string = current_timestamp_as_string()
        sorted_matching_files_list = (
            sorted(matching_files_list,
                   key=lambda file_meta:
                   file_meta[NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY]
                   )
        )

        for matching_file_metadata in sorted_matching_files_list:
            with NamedTemporaryFile() as named_temp_file:
                transform_load_data(
                    matching_file_metadata.get(
                        NamedLiterals.S3_FILE_METADATA_NAME_KEY
                    ),
                    data_config,
                    record_import_timestamp_as_string,
                    named_temp_file.name
                )
                update_object_latest_dates(
                    obj_pattern_with_latest_dates,
                    object_key_pattern,
                    matching_file_metadata.get(
                        NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY
                    ),
                    data_config.state_file_bucket_name,
                    data_config.state_file_object_name
                )


SHOULD_REMAINING_TASK_EXECUTE = ShortCircuitOperator(
    task_id='Should_Remaining_Tasks_Execute',
    python_callable=is_dag_etl_running,
    dag=S3_CSV_ETL_DAG)


NEW_S3_FILE_SENSOR = S3NewKeySensor(
    task_id='s3_key_sensor_task',
    poke_interval=60 * 1,
    timeout=60 * 60 * 24 * 1,
    state_info_extract_from_config_callable=get_stored_state,
    dag=S3_CSV_ETL_DAG)


LOCK_DAGRUN_UPDATE_PREVIOUS_RUNID = create_python_task(
    S3_CSV_ETL_DAG, "Update_Previous_RunID_Variable_Value_For_DagRun_Locking",
    update_prev_run_id_var_val,
)

ETL_CSV = create_python_task(
    S3_CSV_ETL_DAG, "Etl_Csv",
    etl_new_csv_files,
)

# pylint: disable=pointless-statement
(
        SHOULD_REMAINING_TASK_EXECUTE >>
        LOCK_DAGRUN_UPDATE_PREVIOUS_RUNID >>
        NEW_S3_FILE_SENSOR >>
        ETL_CSV
)
