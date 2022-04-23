from data_pipeline.semantic_scholar.semantic_scholar_config import (
    DEFAULT_BATCH_SIZE,
    SemanticScholarConfig
)


ITEM_CONFIG_DICT_1 = {
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'semanticScholar': [item_dict]}


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
