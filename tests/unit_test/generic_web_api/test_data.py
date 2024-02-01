from data_pipeline.generic_web_api.generic_web_api_config import WebApiConfig
from data_pipeline.generic_web_api.generic_web_api_config_typing import WebApiConfigDict


DATASET_1 = 'dataset_1'
TABLE_1 = 'table_1'

MINIMAL_WEB_API_CONFIG_DICT: WebApiConfigDict = {
    'dataPipelineId': 'pipeline_1',
    'gcpProjectName': 'project_1',
    'importedTimestampFieldName': 'imported_timestamp_1',
    'dataset': DATASET_1,
    'table': TABLE_1,
    'dataUrl': {
        'urlExcludingConfigurableParameters': 'url_1'
    }
}


DEP_ENV = 'test'


def get_data_config(
    conf_dict: WebApiConfigDict,
    dep_env: str = DEP_ENV
) -> WebApiConfig:
    return WebApiConfig.from_dict(
        conf_dict,
        deployment_env=dep_env
    )
