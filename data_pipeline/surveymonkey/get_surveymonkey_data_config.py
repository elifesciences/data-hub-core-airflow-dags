from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class SurveyMonkeyDataConfig:

    def __init__(self, data_config: dict,
                 deployment_env: str,
                 env_placeholder: str = "{ENV}"
                 ):
        self.data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env,
            env_placeholder
        )
        self.project_name = self.data_config.get("projectName")
        self.dataset_name = self.data_config.get("datasetName")

        # survey list
        self.survey_list_table_name = (
            self.data_config.get("surveyMonkeySurveyList").get("table")
        )

    def __repr__(self):
        return repr(vars(self))
