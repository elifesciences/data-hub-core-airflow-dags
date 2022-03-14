from typing import Sequence
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from data_pipeline.europepmc.europepmc_config import (
    BigQueryTargetConfig,
    EuropePmcConfig,
    EuropePmcInitialStateConfig,
    EuropePmcSearchConfig,
    EuropePmcSourceConfig,
    EuropePmcStateConfig,
    StateFileConfig
)

import data_pipeline.europepmc.europepmc_pipeline as europepmc_pipeline_module
from data_pipeline.europepmc.europepmc_pipeline import (
    fetch_article_data_from_europepmc_and_load_into_bigquery,
    get_article_response_json_from_api,
    get_request_params_for_source_config,
    get_request_query_for_source_config_and_initial_state,
    iter_article_data,
    iter_article_data_from_response_json,
    save_state_to_s3_for_config
)


DOI_1 = 'doi1'
DOI_2 = 'doi2'


ITEM_RESPONSE_JSON_1 = {
    'doi': DOI_1
}

ITEM_RESPONSE_JSON_2 = {
    'doi': DOI_2
}

SINGLE_ITEM_RESPONSE_JSON_1 = {
    'resultList': {
        'result': [
            ITEM_RESPONSE_JSON_1
        ]
    }
}


API_URL_1 = 'https://api1'


SOURCE_CONFIG_1 = EuropePmcSourceConfig(
    api_url=API_URL_1,
    search=EuropePmcSearchConfig(
        query='query1'
    ),
    fields_to_return=['title', 'doi']
)

TARGET_CONFIG_1 = BigQueryTargetConfig(
    project_name='project1',
    dataset_name='dataset1',
    table_name='table1'
)

INITIAL_STATE_CONFIG_1 = EuropePmcInitialStateConfig(
    start_date_str='2020-01-02'
)

STATE_CONFIG_1 = EuropePmcStateConfig(
    initial_state=INITIAL_STATE_CONFIG_1,
    state_file=StateFileConfig(
        bucket_name='bucket1',
        object_name='object1'
    )
)

CONFIG_1 = EuropePmcConfig(
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1,
    state=STATE_CONFIG_1
)


@pytest.fixture(name='get_article_response_json_from_api_mock')
def _get_article_response_json_from_api_mock():
    with patch.object(europepmc_pipeline_module, 'get_article_response_json_from_api') as mock:
        yield mock


@pytest.fixture(name='upload_s3_object_mock', autouse=True)
def _upload_s3_object_mock():
    with patch.object(europepmc_pipeline_module, 'upload_s3_object') as mock:
        yield mock


@pytest.fixture(name='requests_mock', autouse=True)
def _requests_mock():
    with patch.object(europepmc_pipeline_module, 'requests') as mock:
        yield mock


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        europepmc_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_article_data_mock')
def _iter_article_data_mock():
    with patch.object(europepmc_pipeline_module, 'iter_article_data') as mock:
        yield mock


@pytest.fixture(name='save_state_to_s3_for_config_mock')
def _save_state_to_s3_for_config_mock():
    with patch.object(europepmc_pipeline_module, 'save_state_to_s3_for_config') as mock:
        yield mock


def get_response_json_for_items(items: Sequence[str]) -> dict:
    return {
        'resultList': {
            'result': items
        }
    }


class TestIterArticleDataFromResponseJson:
    def test_should_return_single_item_from_response(self):
        result = list(iter_article_data_from_response_json(SINGLE_ITEM_RESPONSE_JSON_1))
        assert result == [ITEM_RESPONSE_JSON_1]


class TestGetRequestQueryForSourceConfigAndInitialState:
    def test_should_add_first_idate(self):
        original_query = SOURCE_CONFIG_1.search.query
        query = get_request_query_for_source_config_and_initial_state(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        assert query == (
            f"(FIRST_IDATE:'{INITIAL_STATE_CONFIG_1.start_date_str}') {original_query}"
        )


class TestGetRequestParamsForSourceConfig:
    def test_should_include_query_from_config(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        assert params['query'].endswith(SOURCE_CONFIG_1.search.query)

    def test_should_specify_format_json(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        assert params['format'] == 'json'

    def test_should_specify_result_type_core(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        assert params['resultType'] == 'core'


class TestGetArticleResponseJsonFromApi:
    def test_should_pass_url_and_params_to_requests_get(
        self,
        requests_mock: MagicMock
    ):
        get_article_response_json_from_api(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        requests_mock.get.assert_called_with(
            API_URL_1,
            params=get_request_params_for_source_config(
                SOURCE_CONFIG_1,
                INITIAL_STATE_CONFIG_1
            )
        )

    def test_should_return_response_json(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.json.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        actual_response_json = get_article_response_json_from_api(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )
        assert actual_response_json == SINGLE_ITEM_RESPONSE_JSON_1


class TestIterArticleData:
    def test_should_call_api_and_return_articles_from_response(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        result = list(iter_article_data(SOURCE_CONFIG_1, INITIAL_STATE_CONFIG_1))
        assert result == [ITEM_RESPONSE_JSON_1]
        get_article_response_json_from_api_mock.assert_called_with(
            SOURCE_CONFIG_1,
            INITIAL_STATE_CONFIG_1
        )

    def test_should_filter_returned_response_fields(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            {
                'doi': DOI_1,
                'other': 'other1'
            }
        ])
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=['doi']),
            INITIAL_STATE_CONFIG_1
        ))
        assert result == [{
            'doi': DOI_1
        }]

    def test_should_not_filter_returned_response_fields_if_fields_to_return_is_none(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        item_response_1 = {
            'doi': DOI_1,
            'other': 'other1'
        }
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            item_response_1
        ])
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=None),
            INITIAL_STATE_CONFIG_1
        ))
        assert result == [item_response_1]


class TestSaveStateToS3ForConfig:
    def test_should_pass_bucket_and_object_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        save_state_to_s3_for_config(
            STATE_CONFIG_1
        )
        upload_s3_object_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name,
            data_object=ANY
        )

    def test_should_pass_day_after_start_date_str_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        save_state_to_s3_for_config(
            STATE_CONFIG_1._replace(
                initial_state=EuropePmcInitialStateConfig(
                    start_date_str='2020-01-02'
                )
            )
        )
        upload_s3_object_mock.assert_called_with(
            bucket=ANY,
            object_key=ANY,
            data_object='2020-01-03'
        )


class TestFetchArticleDataFromEuropepmcAndLoadIntoBigQuery:
    def test_should_pass_project_dataset_and_table_to_bq_load_method(
        self,
        iter_article_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=TARGET_CONFIG_1.project_name,
            dataset_name=TARGET_CONFIG_1.dataset_name,
            table_name=TARGET_CONFIG_1.table_name,
            json_list=ANY
        )

    def test_should_pass_json_list_to_bq_load_method(
        self,
        iter_article_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [
            ITEM_RESPONSE_JSON_1,
            ITEM_RESPONSE_JSON_2
        ]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=ANY,
            dataset_name=ANY,
            table_name=ANY,
            json_list=json_list
        )

    def test_should_pass_batched_json_list_to_bq_load_method(
        self,
        iter_article_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [
            ITEM_RESPONSE_JSON_1,
            ITEM_RESPONSE_JSON_2
        ]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1._replace(batch_size=1)
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_has_calls([
            call(
                project_name=ANY,
                dataset_name=ANY,
                table_name=ANY,
                json_list=[json_list[0]]
            ),
            call(
                project_name=ANY,
                dataset_name=ANY,
                table_name=ANY,
                json_list=[json_list[1]]
            )
        ])

    def test_should_call_save_state_for_config(
        self,
        iter_article_data_mock: MagicMock,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        iter_article_data_mock.return_value = [ITEM_RESPONSE_JSON_1]
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        save_state_to_s3_for_config_mock.assert_called_with(
            CONFIG_1.state
        )
