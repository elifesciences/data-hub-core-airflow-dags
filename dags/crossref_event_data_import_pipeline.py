"""
dag for  crossref data import into bigquery
"""
import os
import logging
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task,
    get_task_run_instance_fullname,
)
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_last_run_day_from_cloud_storage,
    etl_crossref_data,
    CrossRefimportDataPipelineConfig,
    add_timestamp_field_to_schema,
    current_timestamp_as_string,
)
from data_pipeline.utils.cloud_data_store.bq_data_service import (
    create_table_if_not_exist,
    does_bigquery_table_exist,
    load_file_into_bq,
)
from data_pipeline.utils.cloud_data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
    download_s3_json_object,
    upload_s3_object,
)
# pylint: disable=invalid-name, pointless-statement


LOGGER = logging.getLogger(__name__)
DEFAULT_ARGS = get_default_args()

# below are should be given as environment variables
CROSSREF_CONFIG_S3_BUCKET_NAME = 'CROSSREF_CONFIG_S3_BUCKET'
DEFAULT_CROSSREF_CONFIG_S3_BUCKET_VALUE = "prod-elife-data-pipeline"
CROSSREF_CONFIG_S3_OBJECT_KEY_NAME = 'CROSSREF_CONFIG_S3_OBJECT_KEY'
DEFAULT_CROSSREF_CONFIG_S3_OBJECT_KEY_VALUE = (
    "airflow_test/crossref_event/elife-data-pipeline.config.yaml"
)
CROSS_REF_IMPORT_SCHEDULE_INTERVAL_KEY = 'CROSS_REF_IMPORT_SCHEDULE_INTERVAL'
DEFAULT_CROSS_REF_IMPORT_SCHEDULE_INTERVAL = '@daily'

DEPLOYMENT_ENV = 'DEPLOYMENT_ENV'
DEFAULT_DEPLOYMENT_ENV_VALUE = None


UNTIL_TIME_COLLECTED_PARAM_KEY = "until_collected_date"

def get_env_var_or_use_default(env_var_name, default_value):
    """
    :param env_var_name:
    :param default_value:
    :return:
    """
    return os.getenv(env_var_name, default_value)


dag = DAG(
    dag_id="Load_Crossref_Event_Into_Bigquery",
    default_args=DEFAULT_ARGS,
    schedule_interval=get_env_var_or_use_default(
        CROSS_REF_IMPORT_SCHEDULE_INTERVAL_KEY,
        DEFAULT_CROSS_REF_IMPORT_SCHEDULE_INTERVAL),
    dagrun_timeout=timedelta(
        minutes=60),
)


def get_data_config(**kwargs):
    """
    :param kwargs:
    :return:
    """
    data_config_dict = download_s3_yaml_object_as_json(
        get_env_var_or_use_default(
            CROSSREF_CONFIG_S3_BUCKET_NAME,
            DEFAULT_CROSSREF_CONFIG_S3_BUCKET_VALUE),
        get_env_var_or_use_default(
            CROSSREF_CONFIG_S3_OBJECT_KEY_NAME,
            DEFAULT_CROSSREF_CONFIG_S3_OBJECT_KEY_VALUE),
    )

    data_config = CrossRefimportDataPipelineConfig(data_config_dict)
    dep_env = get_env_var_or_use_default(DEPLOYMENT_ENV,
                                         DEFAULT_DEPLOYMENT_ENV_VALUE)
    env_based_data_config = data_config if dep_env is None \
        else data_config.modify_config_based_on_deployment_env(dep_env)

    kwargs["ti"].xcom_push(key="data_config", value=env_based_data_config)


def create_bq_table_if_not_exist(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dag_context = kwargs["ti"]
    data_config_dict = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")
    data_config = CrossRefimportDataPipelineConfig(data_config_dict)

    schema_json = download_s3_json_object(
        data_config.schema_file_s3_bucket, data_config.schema_file_object_name
    )
    new_schema = add_timestamp_field_to_schema(
        schema_json, data_config.imported_timestamp_field)

    kwargs["ti"].xcom_push(key="data_schema", value=new_schema)

    does_table_exist = does_bigquery_table_exist(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset,
        table_name=data_config.table,
    )
    if not does_table_exist:

        create_table_if_not_exist(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset,
            table_name=data_config.table,
            json_schema=new_schema,
        )


def download_and_semi_transform_crossref_data(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dag_context = kwargs["ti"]
    data_config_dict = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")
    data_config = CrossRefimportDataPipelineConfig(data_config_dict)
    Path(data_config.temp_file_dir).mkdir(parents=True, exist_ok=True)

    data_schema = dag_context.xcom_pull(
        key="data_schema",
        task_ids="create_table_if_not_exist")

    current_timestamp = current_timestamp_as_string()
    latest_journal_download_date = get_last_run_day_from_cloud_storage(
        bucket=data_config.state_file_bucket,
        object_key=data_config.state_file_name_key,
        number_of_previous_day_to_process=data_config.
        number_of_previous_day_to_process,
    )
    task_run_instance_fullname = get_task_run_instance_fullname(kwargs)
    full_temp_file_location = Path.joinpath(
        Path(data_config.temp_file_dir),
        "_".join([task_run_instance_fullname, "downloaded_date"]),
    )
    externally_triggered_parameters = kwargs['dag_run'].conf
    until_collected_date = \
        externally_triggered_parameters.get(UNTIL_TIME_COLLECTED_PARAM_KEY)
    latest_collected_record_date_as_string = etl_crossref_data(
        base_crossref_url=data_config.crossref_event_base_url,
        latest_journal_download_date=latest_journal_download_date,
        publisher_ids=data_config.publisher_ids,
        message_key=data_config.message_key,
        event_key=data_config.event_key,
        imported_timestamp=current_timestamp,
        imported_timestamp_key=data_config.imported_timestamp_field,
        full_temp_file_location=full_temp_file_location,
        schema=data_schema,
        until_collected_date_as_string=until_collected_date
    )

    kwargs["ti"].xcom_push(
        key="full_temp_file_location",
        value=full_temp_file_location)
    kwargs["ti"].xcom_push(
        key="latest_collected_record_date_as_string",
        value=latest_collected_record_date_as_string,
    )


def load_data_to_bigquery(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dag_context = kwargs["ti"]
    downloaded_data_filename = dag_context.xcom_pull(
        key="full_temp_file_location",
        task_ids="download_and_semi_transform_crossref_data",
    )
    data_config_dict = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")
    data_config = CrossRefimportDataPipelineConfig(data_config_dict)

    load_file_into_bq(
        filename=downloaded_data_filename,
        dataset_name=data_config.dataset,
        table_name=data_config.table,
    )


def cleanup_file(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dag_context = kwargs["ti"]
    downloaded_data_filename = dag_context.xcom_pull(
        key="full_temp_file_location",
        task_ids="download_and_semi_transform_crossref_data",
    )
    if os.path.exists(downloaded_data_filename):
        os.remove(downloaded_data_filename)


def log_last_execution_and_cleanup(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dag_context = kwargs["ti"]
    latest_record_date = dag_context.xcom_pull(
        key="latest_collected_record_date_as_string",
        task_ids="download_and_semi_transform_crossref_data",
    )

    data_config_dict = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")
    data_config = CrossRefimportDataPipelineConfig(data_config_dict)
    state_file_name_key = data_config.state_file_name_key
    state_file_bucket = data_config.state_file_bucket
    upload_s3_object(
        bucket=state_file_bucket,
        object_key=state_file_name_key,
        data_object=latest_record_date)

    downloaded_data_filename = dag_context.xcom_pull(
        key="full_temp_file_location",
        task_ids="download_and_semi_transform_crossref_data",
    )
    if os.path.exists(downloaded_data_filename):
        os.remove(downloaded_data_filename)


get_data_config_task = create_python_task(
    dag, "get_data_config", get_data_config, retries=5
)
create_table_if_not_exist_task = create_python_task(
    dag, "create_table_if_not_exist", create_bq_table_if_not_exist, retries=5
)
download_and_semi_transform_crossref_data_task = create_python_task(
    dag,
    "download_and_semi_transform_crossref_data",
    download_and_semi_transform_crossref_data,
)
load_file_into_bq_task = create_python_task(
    dag, "load_data_to_bigquery", load_data_to_bigquery
)

cleanup_task = create_python_task(
    dag, "cleanup_file", cleanup_file, trigger_rule="one_failed"
)
log_last_execution_and_cleanup_task = create_python_task(
    dag,
    "log_last_execution_and_cleanup",
    log_last_execution_and_cleanup,
    trigger_rule="one_success",
)


[
        cleanup_task,
        [cleanup_task, log_last_execution_and_cleanup_task]
        << load_file_into_bq_task
] << download_and_semi_transform_crossref_data_task << \
        create_table_if_not_exist_task << get_data_config_task
