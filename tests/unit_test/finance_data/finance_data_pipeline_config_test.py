from data_pipeline.finance_data.finance_data_pipeline_config import (
    FinanceDataPipelineConfig
)

PROJECT_NAME = 'project_1'
DATASET = 'dataset_1'
TABLE = 'table_1'

SOURCE_CONFIG = {
    'projectName': PROJECT_NAME,
    'dataset': DATASET,
    'table': TABLE
}

BUCKET = 'bucket_1'
OBJECT_NAME = 'object_1.csv'

TARGET_CONFIG = {
    'bucket': BUCKET,
    'objectName': OBJECT_NAME
}

DATA_PIPELINE_ID = 'pipeline_id_1'

ITEM_CONFIG_DICT = {
    'dataPipelineId': DATA_PIPELINE_ID,
    'source': SOURCE_CONFIG,
    'target': TARGET_CONFIG
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'financeDataPipeline': [item_dict]}


CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT)


class TestFinanceDataPipelineConfig:
    def test_should_read_data_pipeline_id(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].data_pipeline_id == DATA_PIPELINE_ID

    def test_should_read_source_project_name(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.project_name == PROJECT_NAME

    def test_should_read_source_dataset(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.dataset == DATASET

    def test_should_read_source_table(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.table == TABLE

    def test_should_read_target_bucket(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].target.bucket == BUCKET

    def test_should_read_target_object_name(self):
        config = FinanceDataPipelineConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].target.object_name == OBJECT_NAME

    def test_should_read_multiple_pipeline_configs(self):
        # Arrange
        additional_pipeline_id = 'pipeline_id_2'
        additional_source_config = {
            'projectName': 'project_2',
            'dataset': 'dataset_2',
            'table': 'table_2'
        }
        additional_target_config = {
            'bucket': 'bucket_2',
            'objectName': 'object_2.csv'
        }
        additional_item_config_dict = {
            'dataPipelineId': additional_pipeline_id,
            'source': additional_source_config,
            'target': additional_target_config
        }
        multiple_config_dict = get_config_for_item_config_dict(ITEM_CONFIG_DICT)
        multiple_config_dict['financeDataPipeline'].append(additional_item_config_dict)

        configs = FinanceDataPipelineConfig.parse_config_list_from_dict(multiple_config_dict)

        assert len(configs) == 2
        assert configs[0].data_pipeline_id == DATA_PIPELINE_ID
        assert configs[0].source.project_name == PROJECT_NAME
        assert configs[0].target.bucket == BUCKET
        assert configs[1].data_pipeline_id == additional_pipeline_id
        assert configs[1].source.project_name == 'project_2'
        assert configs[1].target.bucket == 'bucket_2'
