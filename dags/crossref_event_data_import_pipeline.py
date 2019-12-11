"""
dag for  crossref data import into bigquery
"""
import os
import logging
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from airflow import DAG
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task,
    get_task_run_instance_fullname,
)
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_new_data_download_start_date_from_cloud_storage,
    etl_crossref_data_return_latest_timestamp,
    add_data_hub_timestamp_field_to_bigquery_schema,
    current_timestamp_as_string,
)
from data_pipeline.crossref_event_data.helper_class import (
    CrossRefImportDataPipelineConfig,
    ExternalTriggerConfig,
)
from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    load_file_into_bq,
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
    download_s3_json_object,
    upload_s3_object,
)


LOGGER = logging.getLogger(__name__)
DAG_ID = "Load_Crossref_Event_Into_Bigquery"
# below are should be given as environment variables
CROSSREF_CONFIG_S3_BUCKET_NAME = "CROSSREF_CONFIG_S3_BUCKET"
DEFAULT_CROSSREF_CONFIG_S3_BUCKET_VALUE = "ci-elife-data-pipeline"
CROSSREF_CONFIG_S3_OBJECT_KEY_NAME = "CROSSREF_CONFIG_S3_OBJECT_KEY"
DEFAULT_CROSSREF_CONFIG_S3_OBJECT_KEY_VALUE = (
    "airflow_test/crossref_event/elife-data-pipeline.config.yaml"
)
CROSS_REF_IMPORT_SCHEDULE_INTERVAL_KEY = "CROSS_REF_IMPORT_SCHEDULE_INTERVAL"
DEFAULT_CROSS_REF_IMPORT_SCHEDULE_INTERVAL = None

DEPLOYMENT_ENV = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = None

# keys for  external parameters passed to the dag when extenally triggered


def get_env_var_or_use_default(env_var_name, default_value):
    return os.getenv(env_var_name, default_value)


CROSSREF_DAG = DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    schedule_interval=get_env_var_or_use_default(
        CROSS_REF_IMPORT_SCHEDULE_INTERVAL_KEY,
        DEFAULT_CROSS_REF_IMPORT_SCHEDULE_INTERVAL,
    ),
    dagrun_timeout=timedelta(minutes=60),
)


def get_data_config(**kwargs):
    data_config_dict = download_s3_yaml_object_as_json(
        get_env_var_or_use_default(
            CROSSREF_CONFIG_S3_BUCKET_NAME,
            DEFAULT_CROSSREF_CONFIG_S3_BUCKET_VALUE
        ),
        get_env_var_or_use_default(
            CROSSREF_CONFIG_S3_OBJECT_KEY_NAME,
            DEFAULT_CROSSREF_CONFIG_S3_OBJECT_KEY_VALUE,
        ),
    )

    data_config = CrossRefImportDataPipelineConfig(data_config_dict)
    dep_env = get_env_var_or_use_default(DEPLOYMENT_ENV,
                                         DEFAULT_DEPLOYMENT_ENV_VALUE)
    env_based_data_config = (
        data_config_dict
        if dep_env is None
        else data_config.modify_config_based_on_env(dep_env)
    )

    kwargs["ti"].xcom_push(key="sample_data_config",
                           value=env_based_data_config)


def create_bq_table_if_not_exist(**kwargs):
    dag_context = kwargs["ti"]
    data_config_dict = dag_context.xcom_pull(
        key="sample_data_config", task_ids="get_data_config"
    )
    data_config = CrossRefImportDataPipelineConfig(data_config_dict)

    schema_json = download_s3_json_object(
        data_config.schema_file_s3_bucket, data_config.schema_file_object_name
    )
    new_schema = add_data_hub_timestamp_field_to_bigquery_schema(
        schema_json, data_config.imported_timestamp_field
    )

    kwargs["ti"].xcom_push(key="data_schema", value=new_schema)
    externally_triggered_parameters = kwargs["dag_run"].conf or {}
    dataset = externally_triggered_parameters.get(
        ExternalTriggerConfig.BQ_DATASET_PARAM_KEY, data_config.dataset
    )
    table = externally_triggered_parameters.get(
        ExternalTriggerConfig.BQ_TABLE_PARAM_KEY, data_config.table
    )
    create_table_if_not_exist(
        project_name=data_config.project_name,
        dataset_name=dataset,
        table_name=table,
        json_schema=new_schema,
    )


def crossref_data_etl(**kwargs):
    dag_context = kwargs["ti"]
    data_config = CrossRefImportDataPipelineConfig(
        dag_context.xcom_pull(key="sample_data_config",
                              task_ids="get_data_config")
    )

    data_schema = dag_context.xcom_pull(
        key="data_schema", task_ids="create_table_if_not_exist"
    )

    current_timestamp = current_timestamp_as_string()
    latest_journal_download_date = (
        get_new_data_download_start_date_from_cloud_storage(
            bucket=data_config.state_file_bucket,
            object_key=data_config.state_file_name_key
        )
    )

    task_run_instance_fullname = get_task_run_instance_fullname(kwargs)

    # handles the external triggers
    externally_triggered_parameters = kwargs["dag_run"].conf or {}
    until_collected_date = externally_triggered_parameters.get(
        ExternalTriggerConfig.UNTIL_TIME_COLLECTED_PARAM_KEY
    )
    temp_current_timestamp = externally_triggered_parameters.get(
        ExternalTriggerConfig.CURRENT_TIMESTAMP_PARAM_KEY
    )
    current_timestamp = (
        temp_current_timestamp
        if temp_current_timestamp is not None
        else current_timestamp
    )
    latest_journal_download_date = externally_triggered_parameters.get(
        ExternalTriggerConfig.LATEST_DOWNLOAD_DATE_PARAM_KEY,
        latest_journal_download_date,
    )

    dataset = externally_triggered_parameters.get(
        ExternalTriggerConfig.BQ_DATASET_PARAM_KEY, data_config.dataset
    )
    table = externally_triggered_parameters.get(
        ExternalTriggerConfig.BQ_TABLE_PARAM_KEY, data_config.table
    )
    with TemporaryDirectory() as tempdir:
        full_temp_file_location = Path.joinpath(
            Path(tempdir, "_".join([task_run_instance_fullname,
                                    "downloaded_date"]),)
        )

        latest_collected_record_date_as_string = (
            etl_crossref_data_return_latest_timestamp(
                base_crossref_url=data_config.crossref_event_base_url,
                latest_journal_download_date=latest_journal_download_date,
                journal_doi_prefixes=data_config.publisher_ids,
                message_key=data_config.message_key,
                event_key=data_config.event_key,
                imported_timestamp=current_timestamp,
                imported_timestamp_key=data_config.imported_timestamp_field,
                full_temp_file_location=full_temp_file_location,
                schema=data_schema,
                until_date_as_string=until_collected_date,
            )
        )

        load_file_into_bq(
            filename=full_temp_file_location,
            dataset_name=dataset, table_name=table,
        )

    kwargs["ti"].xcom_push(
        key="latest_collected_record_date_as_string",
        value=latest_collected_record_date_as_string,
    )


def log_last_record_date(**kwargs):
    dag_context = kwargs["ti"]
    latest_record_date = dag_context.xcom_pull(
        key="latest_collected_record_date_as_string",
        task_ids="crossref_event_data_etl",
    )

    data_config_dict = dag_context.xcom_pull(
        key="sample_data_config", task_ids="get_data_config"
    )
    data_config = CrossRefImportDataPipelineConfig(data_config_dict)
    state_file_name_key = data_config.state_file_name_key
    state_file_bucket = data_config.state_file_bucket
    upload_s3_object(
        bucket=state_file_bucket,
        object_key=state_file_name_key,
        data_object=latest_record_date,
    )


# pylint: disable=invalid-name, pointless-statement
get_data_config_task = create_python_task(
    CROSSREF_DAG, "get_data_config", get_data_config, retries=5
)
create_table_if_not_exist_task = create_python_task(
    CROSSREF_DAG, "create_table_if_not_exist",
    create_bq_table_if_not_exist, retries=5
)
crossref_event_data_etl_task = create_python_task(
    CROSSREF_DAG, "crossref_event_data_etl",
    crossref_data_etl,
)
log_last_record_date_task = create_python_task(
    CROSSREF_DAG,
    "log_last_record_date",
    log_last_record_date,
    trigger_rule="one_success",
)

(
    log_last_record_date_task
    << crossref_event_data_etl_task
    << create_table_if_not_exist_task
    << get_data_config_task
)
