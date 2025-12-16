# pylint: disable=too-few-public-methods, too-many-instance-attributes
class SurveyMonkeyDataConfig:

    def __init__(self, data_config: dict):
        self.data_config = data_config
        self.project_name = self.data_config.get('projectName')
        self.dataset_name = self.data_config.get('datasetName')

        # survey list
        self.survey_list_table_name = (
            self.data_config['surveyMonkeySurveyList'].get('table')
        )
        self.survey_id_column_name = (
            self.data_config['surveyMonkeySurveyList'].get('survey_id_column_name')
        )
        # survey ids to request
        self.survey_id_list = self.data_config.get('surveyIdListToRequest')

        # survey details
        self.survey_questions_table_name = (
            self.data_config['surveyMonkeySurveyQuestions'].get('table')
        )
        self.survey_answers_table_name = (
            self.data_config['surveyMonkeySurveyAnswers'].get('table')
        )

    def __repr__(self):
        return repr(vars(self))
