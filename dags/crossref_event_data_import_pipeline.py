# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
import os
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_new_data_download_start_date_from_cloud_storage,
    etl_crossref_data_return_latest_timestamp,
    add_data_hub_timestamp_field_to_bigquery_schema,
)
from data_pipeline.crossref_event_data.helper_class import (
    CrossRefImportDataPipelineConfig,
    ExternalTriggerConfig,
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task,
    get_task_run_instance_fullname,
)
from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    load_file_into_bq,
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_json_object,
    upload_s3_object,
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string
)

LOGGER = logging.getLogger(__name__)
DAG_ID = "Load_Crossref_Event_Into_Bigquery"

CROSSREF_CONFIG_FILE_PATH_ENV_NAME = "CROSSREF_CONFIG_FILE_PATH"
CROSS_REF_IMPORT_SCHEDULE_INTERVAL_ENV_NAME = (
    "CROSS_REF_IMPORT_SCHEDULE_INTERVAL"
)
DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


CROSSREF_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=get_env_var_or_use_default(
        CROSS_REF_IMPORT_SCHEDULE_INTERVAL_ENV_NAME
    ),
    dagrun_timeout=timedelta(minutes=60)
)


def data_config_from_xcom(context):
    dag_context = context["ti"]
    data_config_dict = dag_context.xcom_pull(
        key="data_config_dict", task_ids="get_data_config"
    )
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV)
    data_config = CrossRefImportDataPipelineConfig(
        data_config_dict, deployment_env)
    return data_config


def get_data_config(**kwargs):
    conf_file_path = get_env_var_or_use_default(
        CROSSREF_CONFIG_FILE_PATH_ENV_NAME, ""
    )
    LOGGER.info('conf_file_path: %s', conf_file_path)
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    LOGGER.info('data_config_dict: %s', data_config_dict)
    kwargs["ti"].xcom_push(key="data_config_dict",
                           value=data_config_dict)


def create_bq_table_if_not_exist(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    schema_json = download_s3_json_object(
        data_config.schema_file_s3_bucket,
        data_config.schema_file_object_name
    )
    new_schema = add_data_hub_timestamp_field_to_bigquery_schema(
        schema_json, data_config.imported_timestamp_field
    )

    kwargs["ti"].xcom_push(
        key="data_schema", value=new_schema
    )
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
    data_config = data_config_from_xcom(kwargs)
    dag_context = kwargs["ti"]
    data_schema = dag_context.xcom_pull(
        key="data_schema", task_ids="create_table_if_not_exist"
    )

    current_timestamp = get_current_timestamp_as_string()
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
                message_key=data_config.MESSAGE_KEY,
                event_key=data_config.EVENT_KEY,
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
    data_config = data_config_from_xcom(kwargs)
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
    crossref_data_etl, retries=2
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
