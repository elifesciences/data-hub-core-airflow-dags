from data_pipeline.semantic_scholar.semantic_scholar_config import (
    DEFAULT_BATCH_SIZE,
    SemanticScholarConfig
)


PROJECT_NAME_1 = 'project1'
DATASET_NAME_1 = 'dataset1'
TABLE_NAME_1 = 'table1'


TARGET_1 = {
    'projectName': PROJECT_NAME_1,
    'datasetName': DATASET_NAME_1,
    'tableName': TABLE_NAME_1
}


ITEM_CONFIG_DICT_1 = {
    'target': TARGET_1
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'semanticScholar': [item_dict]}


CONFIG_DICT_1 = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1)


class TestSemanticScholarConfig:
    def test_should_read_batch_size(self):
        config = SemanticScholarConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'batchSize': 123
        }))
        assert config.batch_size == 123

    def test_should_use_default_batch_size(self):
        assert 'batchSize' not in ITEM_CONFIG_DICT_1
        config = SemanticScholarConfig.from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config.batch_size == DEFAULT_BATCH_SIZE

    def test_should_read_target_project_dataset_and_table_name(self):
        config = SemanticScholarConfig.from_dict(CONFIG_DICT_1)
        assert config.target.project_name == PROJECT_NAME_1
        assert config.target.dataset_name == DATASET_NAME_1
        assert config.target.table_name == TABLE_NAME_1
