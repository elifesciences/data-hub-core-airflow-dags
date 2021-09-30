from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


# pylint: disable=too-few-public-methods, too-many-instance-attributes
class MonitoringConfig:

    def __init__(self, data_config: dict,
                 deployment_env_var: str,
                 env_placeholder: str = "{ENV}"
                 ):
        self.data_config = update_deployment_env_placeholder(
            data_config,
            deployment_env_var,
            env_placeholder
        )
        self.project_name = self.data_config.get("project")
        self.dataset_name = self.data_config.get("dataset")
        self.table_name = self.data_config.get("table")

    def __repr__(self):
        return repr(vars(self))
