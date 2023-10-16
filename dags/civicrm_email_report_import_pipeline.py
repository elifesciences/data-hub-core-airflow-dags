# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from data_pipeline.utils.dags.data_pipeline_dag_utils import create_dag, create_python_task
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.civicrm_email_report.civicrm_email_report import iter_email_report
from data_pipeline.utils.data_store.bq_data_service import (
    get_distinct_values_from_bq,
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.civicrm_email_report.get_civicrm_email_report_data_config import (
    CiviCrmEmailReportDataConfig
)
from data_pipeline.utils.pipeline_file_io import read_file_content

LOGGER = logging.getLogger(__name__)

CIVICRM_API_KEY_FILE_PATH_ENV = "CIVICRM_API_KEY_FILE_PATH"
CIVICRM_SITE_KEY_FILE_PATH_ENV = "CIVICRM_SITE_KEY_FILE_PATH"

CIVICRM_EMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME = "CIVICRM_EMAIL_DATA_CONFIG_FILE_PATH"

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"

DAG_ID = "Civicrm_Email_Summary_Report_Pipeline"
CIVICRM_EMAIL_REPORT_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "CIVICRM_EMAIL_REPORT_DATA_PIPELINE_SCHEDULE_INTERVAL"
)


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_data_config(**kwargs):
    config_file_path = os.environ[CIVICRM_EMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME]
    data_config_dictionary = get_yaml_file_as_dict(config_file_path)
    kwargs["ti"].xcom_push(
        key="data_config_dict",
        value=data_config_dictionary
    )


def data_config_from_xcom(context):
    dag_con = context["ti"]
    data_config_dict = dag_con.xcom_pull(
        key="data_config_dict",
        task_ids="get_data_config"
    )
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV
    )
    data_configuration = CiviCrmEmailReportDataConfig(
        data_config_dict, deployment_env)
    LOGGER.info('data_config: %r', data_configuration)
    return data_configuration


def get_civicrm_credential(env_var_name: str):
    return read_file_content(os.environ[env_var_name])


def civicrm_email_report_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    email_id_df = get_distinct_values_from_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name_source=data_config.email_id_source_table,
        column_name=data_config.email_id_column
    )
    email_id_list = email_id_df[0].values.tolist()

    email_reports = iter_email_report(
        url=data_config.civicrm_api_url,
        mail_id_list=email_id_list,
        api_key=get_civicrm_credential(CIVICRM_API_KEY_FILE_PATH_ENV),
        site_key=get_civicrm_credential(CIVICRM_SITE_KEY_FILE_PATH_ENV),
    )
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.table_name,
        json_list=email_reports
    )


CIVICRM_EMAIL_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=os.getenv(
        CIVICRM_EMAIL_REPORT_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME
    ),
    dagrun_timeout=timedelta(days=1)
)


get_data_config_task = create_python_task(
    CIVICRM_EMAIL_DAG,
    "get_data_config",
    get_data_config,
    retries=3
)

civicrm_email_report_etl_task = create_python_task(
    CIVICRM_EMAIL_DAG,
    "civicrm_email_report_etl",
    civicrm_email_report_etl,
    retries=5,
)

# pylint: disable=pointless-statement
get_data_config_task >> civicrm_email_report_etl_task
