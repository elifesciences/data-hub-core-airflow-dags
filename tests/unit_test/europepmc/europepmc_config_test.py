import pytest

from data_pipeline.europepmc.europepmc_config import (
    DEFAULT_BATCH_SIZE,
    EuropePmcConfig
)


API_URL_1 = 'https://api1'

SEARCH_QUERY_1 = 'query 1'

FIELDS_TO_RETURN_1 = ['title', 'doi']

PROJECT_NAME_1 = 'project1'
DATASET_NAME_1 = 'dataset1'
TABLE_NAME_1 = 'table1'

TARGET_1 = {
    'projectName': PROJECT_NAME_1,
    'datasetName': DATASET_NAME_1,
    'tableName': TABLE_NAME_1
}

SOURCE_WITHOUT_FIELDS_TO_RETURN_1 = {
    'apiUrl': API_URL_1,
    'search': {
        'query': SEARCH_QUERY_1
    }
}

SOURCE_WITH_FIELDS_TO_RETURN_1: dict = {
    **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
    'fieldsToReturn': FIELDS_TO_RETURN_1
}


INITIAL_START_DATE_STR_1 = '2020-01-01'

BUCKET_NAME_1 = 'bucket1'
OBJECT_NAME_1 = 'object1'

STATE_CONFIG_DICT_1 = {
    'initialState': {
        'startDate': INITIAL_START_DATE_STR_1
    },
    'stateFile': {
        'bucketName': BUCKET_NAME_1,
        'objectName': OBJECT_NAME_1
    }
}


ITEM_CONFIG_DICT_1 = {
    'source': SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
    'target': TARGET_1,
    'state': STATE_CONFIG_DICT_1
}


def get_config_for_item_config_dict_list(item_dict_list: list) -> dict:
    return {'europePmc': item_dict_list}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return get_config_for_item_config_dict_list([item_dict])


CONFIG_DICT_1 = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1)


class TestEuropePmcConfig:
    def test_should_read_multible_config(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict_list([ITEM_CONFIG_DICT_1, ITEM_CONFIG_DICT_1])
        )
        assert len(config_list) == 2

    def test_should_read_api_url(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(CONFIG_DICT_1)
        assert config_list[0].source.api_url == API_URL_1

    def test_should_read_search_query(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(CONFIG_DICT_1)
        assert config_list[0].source.search.query == SEARCH_QUERY_1

    def test_should_allow_no_query(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': {
                **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
                'search': {}
            }
        }))
        assert config_list[0].source.search.query is None

    def test_should_read_extra_params(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': {
                **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
                'search': {
                    'query': 'query1',
                    'hasXyz': 'Y'
                }
            }
        }))
        assert config_list[0].source.search.query == 'query1'
        assert config_list[0].source.search.extra_params == {'hasXyz': 'Y'}

    def test_should_read_fields_to_return(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': SOURCE_WITH_FIELDS_TO_RETURN_1
        }))
        assert config_list[0].source.fields_to_return == FIELDS_TO_RETURN_1

    def test_should_allow_fields_to_return_not_be_specified(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': SOURCE_WITHOUT_FIELDS_TO_RETURN_1
        }))
        assert config_list[0].source.fields_to_return is None

    def test_should_source_max_days(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': {
                **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
                'maxDays': 123
            }
        }))
        assert config_list[0].source.max_days == 123

    def test_should_default_max_days_to_none(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1
        }))
        assert config_list[0].source.max_days is None

    def test_should_read_target_project_dataset_and_table_name(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(CONFIG_DICT_1)
        assert config_list[0].target.project_name == PROJECT_NAME_1
        assert config_list[0].target.dataset_name == DATASET_NAME_1
        assert config_list[0].target.table_name == TABLE_NAME_1

    def test_should_read_batch_size(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'batchSize': 123
        }))
        assert config_list[0].batch_size == 123

    def test_should_use_default_batch_size(self):
        assert 'batchSize' not in ITEM_CONFIG_DICT_1
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config_list[0].batch_size == DEFAULT_BATCH_SIZE

    def test_should_read_initial_state_start_date(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config_list[0].state.initial_state.start_date_str == INITIAL_START_DATE_STR_1

    def test_should_read_state_file_config(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config_list[0].state.state_file.bucket_name == BUCKET_NAME_1
        assert config_list[0].state.state_file.object_name == OBJECT_NAME_1

    def test_should_default_extract_individual_results_from_response_config_to_true(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config_list[0].source.extract_individual_results_from_response is True

    def test_should_read_extract_individual_results_from_response_config(self):
        config_list = EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': {
                **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
                'extractIndividualResultsFromResponse': False
            }
        }))
        assert config_list[0].source.extract_individual_results_from_response is False

    def test_should_raise_error_if_writing_whole_response_and_using_fields_to_return(self):
        with pytest.raises(AssertionError):
            EuropePmcConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
                **ITEM_CONFIG_DICT_1,
                'source': {
                    **SOURCE_WITH_FIELDS_TO_RETURN_1,
                    'extractIndividualResultsFromResponse': False
                }
            }))
