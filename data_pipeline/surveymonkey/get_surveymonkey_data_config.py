from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class SurveyMonkeyDataConfig:
    project_name: str
    dataset_name: str

    # survey list
    survey_list_table_name: str

    # survey ids to request
    survey_id_list: Sequence[str]

    # survey details
    survey_questions_table_name: str
    survey_answers_table_name: str

    @staticmethod
    def from_dict(data_config: dict) -> 'SurveyMonkeyDataConfig':
        return SurveyMonkeyDataConfig(
            project_name=data_config['projectName'],
            dataset_name=data_config['datasetName'],

            # survey list
            survey_list_table_name=(
                data_config['surveyMonkeySurveyList']['table']
            ),

            # survey ids to request
            survey_id_list=data_config['surveyIdListToRequest'],

            # survey details
            survey_questions_table_name=(
                data_config['surveyMonkeySurveyQuestions']['table']
            ),
            survey_answers_table_name=(
                data_config['surveyMonkeySurveyAnswers']['table']
            )
        )
