# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
import json
from datetime import timedelta

from data_pipeline.surveymonkey.surveymonkey_etl import (
    iter_formated_survey_user_answers,
    get_survey_list,
    get_survey_question_details,
    get_bq_json_for_survey_questions_response_json
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.utils.pipeline_file_io import (
    get_yaml_file_as_dict
)

from data_pipeline.utils.data_store.bq_data_service import (
    get_distinct_values_from_bq,
    load_given_json_list_data_from_tempdir_to_bq
)

from data_pipeline.surveymonkey.get_surveymonkey_data_config import (
    SurveyMonkeyDataConfig
)

LOGGER = logging.getLogger(__name__)

SURVEYMONKEY_SECRET_FILE_ENV_VAR_NAME = "SURVEYMONKEY_SECRET_FILE"
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
    secret_file = get_env_var_or_use_default(SURVEYMONKEY_SECRET_FILE_ENV_VAR_NAME, "")
    LOGGER.info("surveymonkey secret file name %s", secret_file)
    with open(secret_file) as file:
        return json.load(file)["access_token"]


def surveymonkey_survey_list_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    survey_list = get_survey_list(get_surveymonkey_access_token())
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.survey_list_table_name,
        json_list=survey_list
    )


def surveymonkey_survey_questions_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    survey_id_list = get_distinct_values_from_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name_source=data_config.survey_list_table_name,
        column_name=data_config.survey_id_column_name
    ).values.tolist()

    for survey_id in survey_id_list:
        LOGGER.info("questions for survey_id: %s", str(survey_id[1]))
        survey_questions_list = [get_bq_json_for_survey_questions_response_json(
            get_survey_question_details(
                access_token=get_surveymonkey_access_token(),
                survey_id=str(survey_id[1])
            )
        )]
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset_name,
            table_name=data_config.survey_questions_table_name,
            json_list=survey_questions_list
        )


def surveymonkey_survey_answers_etl(**kwargs):
    data_config = data_config_from_xcom(kwargs)
    survey_id_list_from_df = get_distinct_values_from_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name_source=data_config.survey_list_table_name,
        column_name=data_config.survey_id_column_name
    ).values.tolist()

    for survey_id_list in survey_id_list_from_df:
        survey_id = str(survey_id_list[1])
        LOGGER.info("answers for survey_id: %s", survey_id)
        iterable_of_answers_of_one_survey = iter_formated_survey_user_answers(
            access_token=get_surveymonkey_access_token(),
            survey_id=survey_id
        )
        if not iterable_of_answers_of_one_survey:
            LOGGER.info("There is no answer for the survey %s", survey_id)

        load_given_json_list_data_from_tempdir_to_bq(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset_name,
            table_name=data_config.survey_answers_table_name,
            json_list=iterable_of_answers_of_one_survey
        )


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

surveymonkey_survey_questions_task = create_python_task(
    SURVERMONKEY_DAG,
    "surveymonkey_survey_questions_etl",
    surveymonkey_survey_questions_etl,
    retries=3
)

surveymonkey_survey_answers_task = create_python_task(
    SURVERMONKEY_DAG,
    "surveymonkey_survey_answers_etl",
    surveymonkey_survey_answers_etl,
    retries=3
)

# pylint: disable=pointless-statement
(
    get_data_config_task >> surveymonkey_survey_list_task
)
(
    surveymonkey_survey_list_task >> [
        surveymonkey_survey_questions_task,
        surveymonkey_survey_answers_task
    ]
)
