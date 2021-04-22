from unittest.mock import MagicMock
import pytest

from data_pipeline.gmail_data.get_gmail_data import (
    get_dataframe_for_label_response,
    get_dataframe_for_thread_response,
    get_dataframe_for_history_response,
    iter_link_message_thread_ids,
    iter_gmail_history,
    get_label_list,
    get_one_thread
)

USER_ID1 = 'user_id1'
IMPORTED_TIMESTAMP = '2021-04-16'

THREAD_ID1 = 'thread_id1'

START_ID1 = 'start_id1'


@pytest.fixture(name='gmail_service_mock')
def _gmail_service_mock() -> MagicMock:
    return MagicMock(name='service')


@pytest.fixture(name='gmail_label_list_mock')
def _gmail_label_list_mock(gmail_service_mock: MagicMock) -> MagicMock:
    users_mock = gmail_service_mock.users
    labels_mock = users_mock.return_value.labels
    list_mock = labels_mock.return_value.list
    return list_mock


@pytest.fixture(name='gmail_message_list_mock')
def _gmail_message_list_mock(gmail_service_mock: MagicMock) -> MagicMock:
    users_mock = gmail_service_mock.users
    messages_mock = users_mock.return_value.messages
    list_mock = messages_mock.return_value.list
    return list_mock


@pytest.fixture(name='gmail_thread_get_mock')
def _gmail_thread_get_mock(gmail_service_mock: MagicMock) -> MagicMock:
    users_mock = gmail_service_mock.users
    threads_mock = users_mock.return_value.threads
    get_mock = threads_mock.return_value.get
    return get_mock


@pytest.fixture(name='gmail_history_list_mock')
def _gmail_history_list_mock(gmail_service_mock: MagicMock) -> MagicMock:
    users_mock = gmail_service_mock.users
    history_mock = users_mock.return_value.history
    list_mock = history_mock.return_value.list
    return list_mock


class TestGetLabelList:

    def test_should_pass_user_id_to_list_method(
            self,
            gmail_service_mock,
            gmail_label_list_mock):

        get_label_list(gmail_service_mock, USER_ID1)
        gmail_label_list_mock.assert_called_with(userId=USER_ID1)


# pylint: disable=invalid-name
class TestGetDataframeForLabelResponse:

    def test_should_pass_user_id_and_imported_timestamp_in_method(self):
        label_response = {"labels": [{"id": "id1", "name": "name1", "type": "type1"}]}
        df = get_dataframe_for_label_response(label_response, USER_ID1, IMPORTED_TIMESTAMP)
        assert df['user_id'].values == [USER_ID1]
        assert df['imported_timestamp'].values == [IMPORTED_TIMESTAMP]

    def test_should_extract_selected_fields_from_label_response(self):
        label_response = {
            "labels": [
                {
                    "id": "id1",
                    "name": "name1",
                    "type": "type1"
                },
                {
                    "id": "id2",
                    "name": "name2",
                    "type": "type2"
                }
            ]
        }

        df = get_dataframe_for_label_response(label_response, USER_ID1, IMPORTED_TIMESTAMP)
        assert list(df['user_id']) == [USER_ID1, USER_ID1]
        assert list(df['imported_timestamp']) == [IMPORTED_TIMESTAMP, IMPORTED_TIMESTAMP]
        assert list(df['labelId']) == ['id1', 'id2']
        assert list(df['labelName']) == ['name1', 'name2']
        assert list(df['labelType']) == ['type1', 'type2']


class TestIterLinkMessageThreadIds:

    def test_should_pass_user_id_to_list_method(
            self,
            gmail_service_mock,
            gmail_message_list_mock):

        list(iter_link_message_thread_ids(gmail_service_mock, USER_ID1))
        gmail_message_list_mock.assert_called_with(userId=USER_ID1)

    def test_should_yield_first_page_messages_only_without_next_page_token(
            self,
            gmail_service_mock,
            gmail_message_list_mock):

        response = {'messages': ['messages1', 'messages2']}

        execute_mock = gmail_message_list_mock.return_value.execute
        execute_mock.return_value = response

        actual_messages = list(iter_link_message_thread_ids(gmail_service_mock, USER_ID1))

        assert actual_messages == ['messages1', 'messages2']

    def test_should_include_next_page_messages_if_next_page_token_exists(
            self,
            gmail_service_mock,
            gmail_message_list_mock):

        response = {'messages': ['messages1'], 'nextPageToken': 'nextPageToken1'}
        response_in_next_page = {'messages':  ['messages_in_next_page1']}

        execute_mock = gmail_message_list_mock.return_value.execute
        execute_mock.side_effect = [response, response_in_next_page]

        actual_messages = list(iter_link_message_thread_ids(gmail_service_mock, USER_ID1))

        assert actual_messages == ['messages1', 'messages_in_next_page1']


class TestGetDataframeForThreadResponse:

    def test_should_pass_user_id_and_import_timestamp_in_method(self):
        thread_response = {"id": "id1", "historyId": "historyId1", "messages": "messages1"}
        df = get_dataframe_for_thread_response(thread_response, USER_ID1, IMPORTED_TIMESTAMP)
        assert df['user_id'].values == [USER_ID1]
        assert df['imported_timestamp'].values == [IMPORTED_TIMESTAMP]

    def test_should_extract_selected_fields_from_thread_response(self):
        thread_response = {"id": "id1", "historyId": "historyId1", "messages": "messages1"}
        df = get_dataframe_for_thread_response(thread_response, USER_ID1, IMPORTED_TIMESTAMP)
        assert df['threadId'].values == ['id1']
        assert df['historyId'].values == ['historyId1']
        assert df['messages'].values == ['messages1']


class TestGetOneThread:

    def test_should_pass_user_id_and_thread_id_to_get_method(
            self,
            gmail_service_mock,
            gmail_thread_get_mock):

        get_one_thread(gmail_service_mock, USER_ID1, THREAD_ID1)
        gmail_thread_get_mock.assert_called_with(userId=USER_ID1, id=THREAD_ID1)


class TestIterGmailHistory:

    def test_should_pass_user_id_and_start_history_id_to_history_list_method(
            self,
            gmail_service_mock,
            gmail_history_list_mock):

        list(iter_gmail_history(gmail_service_mock, USER_ID1, START_ID1))
        gmail_history_list_mock.assert_called_with(userId=USER_ID1, startHistoryId=START_ID1)

    def test_should_yield_first_page_history_only_without_next_page_token(
            self,
            gmail_service_mock,
            gmail_history_list_mock):

        response = {'history': ['history1']}

        execute_mock = gmail_history_list_mock.return_value.execute
        execute_mock.return_value = response

        actual_messages = list(iter_gmail_history(gmail_service_mock, USER_ID1, START_ID1))
        assert actual_messages == ['history1']

    def test_should_include_next_page_history_if_next_page_token_exists(
            self,
            gmail_service_mock,
            gmail_history_list_mock):

        response = {'history': ['history1'], 'nextPageToken': 'nextPageToken1'}
        response_in_next_page = {'history': ['history_in_next_page1']}

        execute_mock = gmail_history_list_mock.return_value.execute
        execute_mock.side_effect = [response, response_in_next_page]

        actual_messages = list(iter_gmail_history(gmail_service_mock, USER_ID1, START_ID1))

        assert actual_messages == ['history1', 'history_in_next_page1']


class TestGetDataframeForHistoryResponse:

    def test_get_dataframe_for_history_one_response(self):
        history_response = [{'id': 'id1', 'messages': [{'id': 'id2', 'threadId': 'threadId1'}]}]

        df = get_dataframe_for_history_response(history_response, USER_ID1, IMPORTED_TIMESTAMP)
        assert df['threadId'].values == ['threadId1']
        assert df['historyId'].values == ['id1']
        assert df['messages'].values == [{'id': 'id2', 'threadId': 'threadId1'}]
        assert df['user_id'].values == [USER_ID1]
        assert df['imported_timestamp'].values == [IMPORTED_TIMESTAMP]
