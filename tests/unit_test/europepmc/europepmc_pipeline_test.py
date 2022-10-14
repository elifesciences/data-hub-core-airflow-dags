from datetime import date, datetime, timedelta
from typing import Sequence
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from data_pipeline.utils.pipeline_config import (
    BigQueryTargetConfig,
    StateFileConfig
)

from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig,
    EuropePmcInitialStateConfig,
    EuropePmcSearchConfig,
    EuropePmcSourceConfig,
    EuropePmcStateConfig
)

import data_pipeline.europepmc.europepmc_pipeline as europepmc_pipeline_module
from data_pipeline.europepmc.europepmc_pipeline import (
    DEFAULT_CURSOR,
    EuropePmcSearchContext,
    fetch_article_data_from_europepmc_and_load_into_bigquery,
    fetch_article_data_from_europepmc_and_load_into_bigquery_from_config_list,
    get_article_response_json_from_api,
    get_latest_index_date_from_article_data,
    get_next_start_date_str_for_end_date_str,
    get_request_params_for_source_config,
    get_request_query_for_source_config_and_start_date_str,
    get_search_context_for_start_date_str,
    iter_article_data,
    iter_article_data_from_response_json,
    load_state_from_s3_for_config,
    save_state_to_s3_for_config
)
from tests.unit_test.europepmc.europepmc_config_test import INITIAL_START_DATE_STR_1
from tests.unit_test.utils.data_store.bq_data_service_test_utils import (
    create_load_given_json_list_data_from_tempdir_to_bq_mock
)


DOI_1 = 'doi1'
DOI_2 = 'doi2'


CURSOR_1 = 'cursor1'
CURSOR_2 = 'cursor2'

MOCK_TODAY_STR = '2021-01-02'
MOCK_YESTERDAY_STR = (date.fromisoformat(MOCK_TODAY_STR) - timedelta(days=1)).isoformat()

MOCK_YESTERDAY_DATE = date.fromisoformat(MOCK_YESTERDAY_STR)

MOCK_UTC_NOW_STR = MOCK_TODAY_STR + 'T12:34:56'


ITEM_RESPONSE_JSON_1 = {
    'doi': DOI_1,
    'firstIndexDate': MOCK_YESTERDAY_STR
}

ITEM_RESPONSE_JSON_2 = {
    'doi': DOI_2,
    'firstIndexDate': MOCK_YESTERDAY_STR
}

PROVENANCE_1 = {'provenance_key': 'provenance1'}

SINGLE_ITEM_RESPONSE_JSON_1 = {
    'resultList': {
        'result': [
            ITEM_RESPONSE_JSON_1
        ]
    }
}

EMPTY_PAGE_RESPONSE_JSON = {
    'hitCount': 0,
    'resultList': {
        'result': []
    }
}

API_URL_1 = 'https://api1'


SOURCE_CONFIG_1 = EuropePmcSourceConfig(
    api_url=API_URL_1,
    search=EuropePmcSearchConfig(
        query='query1'
    ),
    fields_to_return=['title', 'doi', 'firstIndexDate']
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


SEARCH_CONTEXT_1 = EuropePmcSearchContext(
    start_date_str=INITIAL_START_DATE_STR_1,
    end_date_str=INITIAL_START_DATE_STR_1
)


@pytest.fixture(name='get_article_response_json_from_api_mock')
def _get_article_response_json_from_api_mock():
    with patch.object(europepmc_pipeline_module, 'get_article_response_json_from_api') as mock:
        yield mock


@pytest.fixture(name='upload_s3_object_mock', autouse=True)
def _upload_s3_object_mock():
    with patch.object(europepmc_pipeline_module, 'upload_s3_object') as mock:
        yield mock


@pytest.fixture(name='download_s3_object_as_string_or_file_not_found_error_mock', autouse=True)
def _download_s3_object_as_string_or_file_not_found_error_mock():
    with patch.object(
        europepmc_pipeline_module,
        'download_s3_object_as_string_or_file_not_found_error'
    ) as mock:
        mock.return_value = STATE_CONFIG_1.initial_state.start_date_str
        yield mock


@pytest.fixture(name='get_response_json_with_provenance_from_api_mock', autouse=True)
def _get_response_json_with_provenance_from_api_mock():
    with patch.object(
        europepmc_pipeline_module,
        'get_response_json_with_provenance_from_api'
    ) as mock:
        yield mock


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        europepmc_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq',
        create_load_given_json_list_data_from_tempdir_to_bq_mock()
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


@pytest.fixture(name='fetch_article_data_from_europepmc_and_load_into_bigquery_mock')
def _fetch_article_data_from_europepmc_and_load_into_bigquery_mock():
    with patch.object(
        europepmc_pipeline_module,
        'fetch_article_data_from_europepmc_and_load_into_bigquery'
    ) as mock:
        yield mock


@pytest.fixture(name='date_mock', autouse=True)
def _date_mock():
    with patch.object(europepmc_pipeline_module, 'date') as mock:
        mock.today.return_value = date.fromisoformat(MOCK_TODAY_STR)
        mock.fromisoformat.side_effect = date.fromisoformat
        mock.side_effect = date
        yield mock


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(europepmc_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


def get_response_json_for_items(items: Sequence[str], **kwargs) -> dict:
    return {
        **kwargs,
        'resultList': {
            'result': items
        }
    }


class TestIterArticleDataFromResponseJson:
    def test_should_return_single_item_from_response(self):
        result = list(iter_article_data_from_response_json(
            get_response_json_for_items(
                [ITEM_RESPONSE_JSON_1],
                provenance=PROVENANCE_1
            )
        ))
        assert result == [{**ITEM_RESPONSE_JSON_1, 'provenance': PROVENANCE_1}]


class TestGetRequestQueryForSourceConfigAndInitialState:
    def test_should_add_first_idate(self):
        original_query = SOURCE_CONFIG_1.search.query
        query = get_request_query_for_source_config_and_start_date_str(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1
        )
        date_period_str = (
            f"[{SEARCH_CONTEXT_1.start_date_str} TO {SEARCH_CONTEXT_1.end_date_str}]"
        )
        assert query == (
            f"(FIRST_IDATE:{date_period_str})"
            + f' ({original_query})'
        )

    def test_should_return_first_idate_for_none_query(self):
        query = get_request_query_for_source_config_and_start_date_str(
            SOURCE_CONFIG_1._replace(
                search=EuropePmcSearchConfig(
                    query=None
                )
            ),
            SEARCH_CONTEXT_1
        )
        date_period_str = (
            f"[{SEARCH_CONTEXT_1.start_date_str} TO {SEARCH_CONTEXT_1.end_date_str}]"
        )
        assert query == (
            f"(FIRST_IDATE:{date_period_str})"
        )


class TestGetRequestParamsForSourceConfig:
    def test_should_include_query_from_config(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1
        )
        assert SOURCE_CONFIG_1.search.query in params['query']

    def test_should_specify_format_json(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1
        )
        assert params['format'] == 'json'

    def test_should_specify_result_type_core(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1
        )
        assert params['resultType'] == 'core'

    def test_should_include_extra_params(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1._replace(
                search=EuropePmcSearchConfig(
                    query='query1',
                    extra_params={'hasXyz': 'Y'}
                )
            ),
            SEARCH_CONTEXT_1
        )
        assert params['hasXyz'] == 'Y'

    def test_should_include_cursor_mark(self):
        params = get_request_params_for_source_config(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            cursor=CURSOR_1
        )
        assert params['cursorMark'] == CURSOR_1


class TestGetArticleResponseJsonFromApi:
    def test_should_pass_url_and_params_to_requests_get(
        self,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            provenance=PROVENANCE_1
        )
        get_response_json_with_provenance_from_api_mock.assert_called_with(
            API_URL_1,
            params=get_request_params_for_source_config(
                SOURCE_CONFIG_1,
                SEARCH_CONTEXT_1
            ),
            provenance=PROVENANCE_1
        )

    def test_should_return_response_json(
        self,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_response_json_with_provenance_from_api_mock.return_value = SINGLE_ITEM_RESPONSE_JSON_1
        actual_response_json = get_article_response_json_from_api(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1
        )
        assert actual_response_json == SINGLE_ITEM_RESPONSE_JSON_1


class TestIterArticleData:
    def test_should_call_api_and_return_articles_from_response(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            ITEM_RESPONSE_JSON_1
        ], provenance=PROVENANCE_1)
        result = list(iter_article_data(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            provenance=PROVENANCE_1
        ))
        assert result == [{**ITEM_RESPONSE_JSON_1, 'provenance': PROVENANCE_1}]
        get_article_response_json_from_api_mock.assert_called_with(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            cursor=DEFAULT_CURSOR,
            provenance=PROVENANCE_1
        )

    def test_should_call_api_and_return_whole_response_when_not_extracting_indivdual_results(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        response_json = get_response_json_for_items([
            ITEM_RESPONSE_JSON_1
        ], provenance=PROVENANCE_1)
        get_article_response_json_from_api_mock.return_value = response_json
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(
                extract_individual_results_from_response=False,
                fields_to_return=[]
            ),
            SEARCH_CONTEXT_1,
            provenance=PROVENANCE_1
        ))
        assert result == [response_json]

    def test_should_filter_returned_response_fields(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = get_response_json_for_items([
            {
                'doi': DOI_1,
                'other': 'other1'
            }
        ], provenance=PROVENANCE_1)
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=['doi']),
            SEARCH_CONTEXT_1
        ))
        assert result == [{
            'doi': DOI_1,
            'provenance': PROVENANCE_1
        }]

    def test_should_not_remove_provenance(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.return_value = {
            **get_response_json_for_items([{
                'doi': DOI_1,
                'other': 'other1',
            }]),
            'provenance': PROVENANCE_1
        }
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=['doi']),
            SEARCH_CONTEXT_1
        ))
        assert result == [{
            'doi': DOI_1,
            'provenance': PROVENANCE_1
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
        ], provenance=PROVENANCE_1)
        result = list(iter_article_data(
            SOURCE_CONFIG_1._replace(fields_to_return=None),
            SEARCH_CONTEXT_1
        ))
        assert result == [{**item_response_1, 'provenance': PROVENANCE_1}]

    def test_should_call_api_with_next_cursor_api_and_return_articles_from_responses(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.side_effect = [
            get_response_json_for_items([
                ITEM_RESPONSE_JSON_1
            ], provenance=PROVENANCE_1, nextCursorMark=CURSOR_1),
            get_response_json_for_items([
                ITEM_RESPONSE_JSON_2
            ], provenance=PROVENANCE_1)
        ]
        result = list(iter_article_data(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            provenance=PROVENANCE_1
        ))
        assert result == [
            {**ITEM_RESPONSE_JSON_1, 'provenance': PROVENANCE_1},
            {**ITEM_RESPONSE_JSON_2, 'provenance': PROVENANCE_1}
        ]
        get_article_response_json_from_api_mock.assert_has_calls([
            call(
                SOURCE_CONFIG_1,
                SEARCH_CONTEXT_1,
                cursor=DEFAULT_CURSOR,
                provenance=PROVENANCE_1
            ),
            call(
                SOURCE_CONFIG_1,
                SEARCH_CONTEXT_1,
                cursor=CURSOR_1,
                provenance=PROVENANCE_1
            )
        ])

    def test_should_ignore_next_cursor_if_same_as_previous_cursor(
        self,
        get_article_response_json_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api_mock.side_effect = [
            get_response_json_for_items([
                ITEM_RESPONSE_JSON_1
            ], provenance=PROVENANCE_1, nextCursorMark=CURSOR_1),
            get_response_json_for_items([
                ITEM_RESPONSE_JSON_2
            ], provenance=PROVENANCE_1, nextCursorMark=CURSOR_1)
        ]
        result = list(iter_article_data(
            SOURCE_CONFIG_1,
            SEARCH_CONTEXT_1,
            provenance=PROVENANCE_1
        ))
        assert result == [
            {**ITEM_RESPONSE_JSON_1, 'provenance': PROVENANCE_1},
            {**ITEM_RESPONSE_JSON_2, 'provenance': PROVENANCE_1}
        ]
        get_article_response_json_from_api_mock.assert_has_calls([
            call(
                SOURCE_CONFIG_1,
                SEARCH_CONTEXT_1,
                cursor=DEFAULT_CURSOR,
                provenance=PROVENANCE_1
            ),
            call(
                SOURCE_CONFIG_1,
                SEARCH_CONTEXT_1,
                cursor=CURSOR_1,
                provenance=PROVENANCE_1
            )
        ])


class TestSaveStateToS3ForConfig:
    def test_should_pass_bucket_and_object_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        save_state_to_s3_for_config(
            STATE_CONFIG_1,
            STATE_CONFIG_1.initial_state.start_date_str
        )
        upload_s3_object_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name,
            data_object=ANY
        )

    def test_should_passed_in_start_date_str_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        save_state_to_s3_for_config(
            STATE_CONFIG_1,
            '2020-01-02'
        )
        upload_s3_object_mock.assert_called_with(
            bucket=ANY,
            object_key=ANY,
            data_object='2020-01-02'
        )


class TestLoadStateFromS3ForConfig:
    def test_should_call_download_s3_object_as_string(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        result = load_state_from_s3_for_config(
            STATE_CONFIG_1
        )
        download_s3_object_as_string_or_file_not_found_error_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name
        )
        assert result == download_s3_object_as_string_or_file_not_found_error_mock.return_value

    def test_should_return_initial_state_if_file_does_not_exist(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.side_effect = (
            FileNotFoundError()
        )
        result = load_state_from_s3_for_config(
            STATE_CONFIG_1
        )
        download_s3_object_as_string_or_file_not_found_error_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name
        )
        assert result == INITIAL_STATE_CONFIG_1.start_date_str


class TestGetSearchContextForStartDateStr:
    def test_should_set_given_start_date(self):
        result = get_search_context_for_start_date_str('2001-02-03')
        assert result.start_date_str == '2001-02-03'

    def test_should_set_end_date_to_max_days_ahead(self):
        result = get_search_context_for_start_date_str(
            '2001-02-03',
            max_days=10
        )
        assert result.end_date_str == '2001-02-12'

    def test_should_use_yesterday_as_end_date_if_max_days_beyond_yesterday(
        self,
        date_mock: MagicMock
    ):
        date_mock.today.return_value = date(2001, 2, 8)
        result = get_search_context_for_start_date_str(
            '2001-02-03',
            max_days=10
        )
        assert result.end_date_str == '2001-02-07'

    def test_should_use_yesterday_as_end_date_if_max_days_not_specified(
        self,
        date_mock: MagicMock
    ):
        date_mock.today.return_value = date(2001, 2, 8)
        result = get_search_context_for_start_date_str(
            '2001-02-03',
            max_days=None
        )
        assert result.end_date_str == '2001-02-07'


class TestGetNextStartDateStrForEndDateStr:
    def test_should_add_one_day(self):
        assert get_next_start_date_str_for_end_date_str(
            '2001-02-03'
        ) == '2001-02-04'


class TestGetLatestIndexDateFromArticleData:
    def test_should_return_none_if_the_nested_list_is_empty(self):
        assert get_latest_index_date_from_article_data(
            {
                'resultList': {
                    'result': []
                }
            },
            source_config=SOURCE_CONFIG_1._replace(
                extract_individual_results_from_response=False
            )
        ) is None

    def test_should_raise_an_error_if_first_index_date_not_found_in_data(self):
        with pytest.raises(KeyError):
            get_latest_index_date_from_article_data(
                {'other': 'other value'},
                source_config=SOURCE_CONFIG_1
            )

    def test_should_return_latest_first_index_date_from_nested_list_with_multiple_dates(self):
        assert get_latest_index_date_from_article_data(
            {
                'resultList': {
                    'result': [
                        {'firstIndexDate': '2001-02-03'},
                        {'firstIndexDate': '2001-02-05'},
                        {'firstIndexDate': '2001-02-04'}
                    ]
                }
            },
            source_config=SOURCE_CONFIG_1._replace(
                extract_individual_results_from_response=False
            )
        ) == date.fromisoformat('2001-02-05')


class TestFetchArticleDataFromEuropepmcAndLoadIntoBigQuery:
    def test_should_pass_provenance_to_iter_article_data(
        self,
        iter_article_data_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        iter_article_data_mock.assert_called_with(
            ANY,
            ANY,
            provenance={'imported_timestamp': MOCK_UTC_NOW_STR}
        )

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
        assert (
            load_given_json_list_data_from_tempdir_to_bq_mock.latest_call_json_list
            == json_list
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
        assert load_given_json_list_data_from_tempdir_to_bq_mock.calls_json_lists == [
            [json_list[0]],
            [json_list[1]]
        ]

    def test_should_not_call_save_state_for_empty_result(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            STATE_CONFIG_1.initial_state.start_date_str
        )
        iter_article_data_mock.return_value = [EMPTY_PAGE_RESPONSE_JSON]
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1._replace(
                source=SOURCE_CONFIG_1._replace(extract_individual_results_from_response=False)
            )
        )
        save_state_to_s3_for_config_mock.assert_not_called()

    def test_should_update_state_with_latest_index_date_plus_one(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        latest_first_index_date = MOCK_YESTERDAY_DATE - timedelta(days=9)
        expected_next_start_date = latest_first_index_date + timedelta(days=1)

        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            (MOCK_YESTERDAY_DATE - timedelta(days=10)).isoformat()
        )
        iter_article_data_mock.return_value = [{
            'firstIndexDate': (latest_first_index_date).isoformat()
        }]
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1._replace(
                source=SOURCE_CONFIG_1._replace(extract_individual_results_from_response=True)
            )
        )
        save_state_to_s3_for_config_mock.assert_called_with(
            CONFIG_1.state,
            expected_next_start_date.isoformat()
        )

    def test_should_reject_index_date_before_start_date(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock
    ):
        initial_start_date = MOCK_YESTERDAY_DATE - timedelta(days=10)

        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            initial_start_date.isoformat()
        )
        iter_article_data_mock.return_value = [{
            'firstIndexDate': (initial_start_date - timedelta(days=1)).isoformat()
        }]
        with pytest.raises(AssertionError):
            fetch_article_data_from_europepmc_and_load_into_bigquery(
                CONFIG_1._replace(
                    source=SOURCE_CONFIG_1._replace(extract_individual_results_from_response=True)
                )
            )

    def test_should_reject_index_date_after_end_date(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock
    ):
        initial_start_date = MOCK_YESTERDAY_DATE - timedelta(days=10)

        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            initial_start_date.isoformat()
        )
        iter_article_data_mock.return_value = [{
            'firstIndexDate': (MOCK_YESTERDAY_DATE + timedelta(days=1)).isoformat()
        }]
        with pytest.raises(AssertionError):
            fetch_article_data_from_europepmc_and_load_into_bigquery(
                CONFIG_1._replace(
                    source=SOURCE_CONFIG_1._replace(extract_individual_results_from_response=True)
                )
            )

    def test_should_remove_empty_list_before_loading_into_bigquery(
        self,
        iter_article_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        non_empty_value_json_list = [ITEM_RESPONSE_JSON_1]
        raw_json_list = [{
            **ITEM_RESPONSE_JSON_1,
            'empty_list': []
        }]
        iter_article_data_mock.return_value = raw_json_list
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        assert (
            load_given_json_list_data_from_tempdir_to_bq_mock.latest_call_json_list
            == non_empty_value_json_list
        )

    def test_should_call_save_state_for_config(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            STATE_CONFIG_1.initial_state.start_date_str
        )
        iter_article_data_mock.return_value = [ITEM_RESPONSE_JSON_1]
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        expected_next_start_date_str = get_next_start_date_str_for_end_date_str(
            MOCK_YESTERDAY_STR
        )
        save_state_to_s3_for_config_mock.assert_called_with(
            CONFIG_1.state,
            expected_next_start_date_str
        )

    def test_should_not_call_api_if_start_date_is_today(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            MOCK_TODAY_STR
        )
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1
        )
        iter_article_data_mock.assert_not_called()

    def test_should_load_data_into_bigquery_in_batches_and_update_state_for_each_batch(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock,
        iter_article_data_mock: MagicMock,
        save_state_to_s3_for_config_mock: MagicMock
    ):
        initial_start_date_str = (
            (date.fromisoformat(MOCK_TODAY_STR) - timedelta(days=2)).isoformat()
        )
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = (
            initial_start_date_str
        )
        iter_article_data_mock.side_effect = [
            [{**ITEM_RESPONSE_JSON_1, 'firstIndexDate': initial_start_date_str}],
            [{**ITEM_RESPONSE_JSON_2, 'firstIndexDate': MOCK_YESTERDAY_STR}]
        ]
        fetch_article_data_from_europepmc_and_load_into_bigquery(
            CONFIG_1._replace(
                source=SOURCE_CONFIG_1._replace(
                    max_days=1
                )
            )
        )
        save_state_to_s3_for_config_mock.assert_has_calls([
            call(CONFIG_1.state, MOCK_YESTERDAY_STR),
            call(CONFIG_1.state, MOCK_TODAY_STR)
        ])


class TestFetchArticleDataFromEuropepmcAndLoadIntoBigqueryFromConfigList:
    def test_should_call_fetch_article_data_function_for_each_item_in_config_list(
        self,
        fetch_article_data_from_europepmc_and_load_into_bigquery_mock: MagicMock
    ):
        config_list = [CONFIG_1, CONFIG_1]
        fetch_article_data_from_europepmc_and_load_into_bigquery_from_config_list(
            config_list
        )
        assert fetch_article_data_from_europepmc_and_load_into_bigquery_mock.call_count == 2
        fetch_article_data_from_europepmc_and_load_into_bigquery_mock.assert_has_calls([
            call(config_list[0]),
            call(config_list[1])
        ])
