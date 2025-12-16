import argparse
import json
import logging
import os
from typing import Optional, Sequence

from data_pipeline.surveymonkey.get_surveymonkey_data_config import SurveyMonkeyDataConfig
from data_pipeline.surveymonkey.surveymonkey_etl import (
    get_bq_json_for_survey_questions_response_json,
    get_survey_list,
    get_survey_question_details,
    iter_formated_survey_user_answers
)
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


class SurveyMonkeyPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'SURVEYMONKEY_DATA_CONFIG_FILE_PATH'
    SECRET_FILE = 'SURVEYMONKEY_SECRET_FILE'


def get_pipeline_config() -> 'SurveyMonkeyDataConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        SurveyMonkeyPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        SurveyMonkeyDataConfig.from_dict
    )


def get_surveymonkey_access_token() -> str:
    secret_file = os.environ[SurveyMonkeyPipelineEnvironmentVariables.SECRET_FILE]
    LOGGER.info('surveymonkey secret file name %s', secret_file)
    with open(secret_file, encoding='UTF-8') as file:
        return json.load(file)['access_token']


def surveymonkey_survey_list_etl(
    data_config: SurveyMonkeyDataConfig,
    surveymonkey_access_token: str
):
    LOGGER.info('Retrieving Survey List...')
    survey_list = get_survey_list(surveymonkey_access_token)
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=data_config.project_name,
        dataset_name=data_config.dataset_name,
        table_name=data_config.survey_list_table_name,
        json_list=survey_list
    )


def surveymonkey_survey_questions_etl(
    data_config: SurveyMonkeyDataConfig,
    surveymonkey_access_token: str
):
    LOGGER.info('Retrieving Survey Questions..')
    for survey_id in data_config.survey_id_list:
        LOGGER.info('questions for survey_id: %s', str(survey_id))
        survey_questions_list = [
            get_bq_json_for_survey_questions_response_json(
                get_survey_question_details(
                    access_token=surveymonkey_access_token,
                    survey_id=str(survey_id)
                )
            )
        ]
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset_name,
            table_name=data_config.survey_questions_table_name,
            json_list=survey_questions_list
        )


def surveymonkey_survey_answers_etl(
    data_config: SurveyMonkeyDataConfig,
    surveymonkey_access_token: str
):
    LOGGER.info('Retrieving Survey Answers..')
    for survey_id in data_config.survey_id_list:
        survey_id = str(survey_id)
        LOGGER.info('answers for survey_id: %s', survey_id)
        iterable_of_answers_of_one_survey = iter_formated_survey_user_answers(
            access_token=surveymonkey_access_token,
            survey_id=survey_id
        )

        load_given_json_list_data_from_tempdir_to_bq(
            project_name=data_config.project_name,
            dataset_name=data_config.dataset_name,
            table_name=data_config.survey_answers_table_name,
            json_list=iterable_of_answers_of_one_survey
        )


def main(argv: Optional[Sequence[str]] = None):
    # Name CLI and declare no arguments
    parser = argparse.ArgumentParser(description='Run ETL for a Survey Monkey pipeline')
    parser.parse_args(argv)

    pipeline_config = get_pipeline_config()
    LOGGER.info('pipeline_config: %r', pipeline_config)

    surveymonkey_access_token = get_surveymonkey_access_token()

    LOGGER.info('Starting ETL')
    surveymonkey_survey_list_etl(
        data_config=pipeline_config,
        surveymonkey_access_token=surveymonkey_access_token
    )
    surveymonkey_survey_questions_etl(
        data_config=pipeline_config,
        surveymonkey_access_token=surveymonkey_access_token
    )
    surveymonkey_survey_answers_etl(
        data_config=pipeline_config,
        surveymonkey_access_token=surveymonkey_access_token
    )
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
