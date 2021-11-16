# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
import json
from datetime import timedelta
from tempfile import TemporaryDirectory

from data_pipeline.surveymonkey.surveymonkey_etl import (
    get_survey_list
)

from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.utils.pipeline_file_io import (
    get_yaml_file_as_dict
)

from data_pipeline.utils.data_store.bq_data_service import (
    load_file_into_bq,
    create_or_extend_table_schema
)

from data_pipeline.surveymonkey.get_surveymonkey_data_config import (
    SurveyMonkeyDataConfig
)

LOGGER = logging.getLogger(__name__)

SURVEYMONKEY_SECRET_FILE_ENV_VAR = "SURVEYMONKEY_SECRET_FILE"
SURVEYMONKEY_DATA_CONFIG_FILE_PATH_ENV_NAME = "SURVEYMONKEY_DATA_CONFIG_FILE_PATH"

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"

DAG_ID = "SurveyMonkey_Data_Import_Pipeline"
SURVEYMONKEY_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "SURVEYMONKEY_DATA_PIPELINE_SCHEDULE_INTERVAL"
)


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_data_config(**kwargs):
    config_file_path = get_env_var_or_use_default(
        SURVEYMONKEY_DATA_CONFIG_FILE_PATH_ENV_NAME, ""
    )
    data_config_dict = get_yaml_file_as_dict(config_file_path)
    kwargs["ti"].xcom_push(
        key="data_config_dict",
        value=data_config_dict
    )


def data_config_from_xcom(context):
    dag_cont = context["ti"]
    data_config_dict = dag_cont.xcom_pull(
        key="data_config_dict",
        task_ids="get_data_config"
    )
    deployment_env = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV
    )
    data_conf = SurveyMonkeyDataConfig(
        data_config_dict, deployment_env)
    LOGGER.info('data_config: %r', data_conf)
    return data_conf


def get_surveymonkey_access_token():
    secret_file = get_env_var_or_use_default(SURVEYMONKEY_SECRET_FILE_ENV_VAR, "")
    LOGGER.info("surveymonkey secret file name %s", secret_file)
    with open(secret_file) as file:
        return json.load(file)["access_token"]


def surveymonkey_survey_list_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)

    survey_list = get_survey_list(get_surveymonkey_access_token())
    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, 'tmp_file.json')
        LOGGER.info(type(filename))
        LOGGER.info(type(survey_list))
        write_jsonl_to_file(
            json_list=survey_list,
            full_temp_file_location=filename,
        )
        if os.path.getsize(filename) > 0:
            create_or_extend_table_schema(
                gcp_project=data_config.project_name,
                dataset_name=data_config.dataset_name,
                table_name=data_config.survey_list_table_name,
                full_file_location=filename,
                quoted_values_are_strings=True
            )
            load_file_into_bq(
                project_name=data_config.project_name,
                dataset_name=data_config.dataset_name,
                table_name=data_config.survey_list_table_name,
                filename=filename
            )
            LOGGER.info('Loaded table: %s', data_config.survey_list_table_name)
        else:
            LOGGER.info('No updates found for the table: %s', data_config.survey_list_table_name)


SURVERMONKEY_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        SURVEYMONKEY_DATA_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME
    ),
    dagrun_timeout=timedelta(days=1)
)

get_data_config_task = create_python_task(
    SURVERMONKEY_DAG,
    "get_data_config",
    get_data_config,
    retries=3
)

surveymonkey_survey_list_task = create_python_task(
    SURVERMONKEY_DAG,
    "surveymonkey_survey_list_etl",
    surveymonkey_survey_list_etl,
    retries=3
)

# pylint: disable=pointless-statement
get_data_config_task >> surveymonkey_survey_list_task
