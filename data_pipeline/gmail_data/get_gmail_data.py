import datetime
from typing import Iterable
import logging
from itertools import islice
import pandas as pd
import backoff

from google.oauth2 import service_account

from googleapiclient.discovery import build
from googleapiclient.discovery import Resource


LOGGER = logging.getLogger(__name__)


def get_gmail_service_for_user_id(secret_file: str, scopes: str, user_id: str) -> Resource:
    credentials = service_account.Credentials.from_service_account_file(
        secret_file, scopes=scopes)
    delegated_credentials = credentials.with_subject(user_id)

    service = build("gmail", "v1", credentials=delegated_credentials)
    return service


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def get_label_list(service: Resource, user_id: str) -> pd.DataFrame:
    imported_timestamp = get_current_timestamp()
    label_response = service.users().labels().list(userId=user_id).execute()
    return get_dataframe_for_label_response(label_response, user_id, imported_timestamp)


def get_dataframe_for_label_response(
        label_response: dict,
        user_id: str,
        imported_timestamp: str) -> pd.DataFrame:

    df_one_label = pd.DataFrame()
    df_label = pd.DataFrame()
    for label in label_response['labels']:
        df_one_label['labelId'] = [label['id']]
        df_one_label['labelName'] = [label['name']]
        df_one_label['labelType'] = [label['type']]
        df_label = df_label.append(df_one_label)

    df_label['user_id'] = user_id
    df_label['imported_timestamp'] = imported_timestamp

    return df_label


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def iter_link_message_thread_ids(service: Resource, user_id: str) -> Iterable[dict]:
    response = service.users().messages().list(userId=user_id).execute()
    if 'messages' in response:
        yield from response['messages']
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId=user_id, pageToken=page_token).execute()
        yield from response['messages']


def get_link_message_thread_ids(
        service: Resource,
        user_id: str,
        is_gmail_end2end_test: bool) -> pd.DataFrame:
    imported_timestamp = get_current_timestamp()
    messages_iterable = iter_link_message_thread_ids(service, user_id)
    if is_gmail_end2end_test:
        messages_iterable = islice(messages_iterable, 4)
    df_link = pd.DataFrame(messages_iterable)
    df_link['user_id'] = user_id
    df_link['imported_timestamp'] = imported_timestamp
    return df_link


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def get_one_thread(service: str, user_id: str, thread_id: str) -> pd.DataFrame:
    imported_timestamp = get_current_timestamp()
    thread_response = service.users().threads().get(userId=user_id, id=thread_id).execute()
    return get_dataframe_for_thread_response(thread_response, user_id, imported_timestamp)


def get_dataframe_for_thread_response(
        thread_response: dict,
        user_id: str,
        imported_timestamp: str) -> pd.DataFrame:

    df_thread = pd.DataFrame()

    df_thread['threadId'] = [thread_response['id']]
    df_thread['historyId'] = [thread_response['historyId']]
    df_thread['user_id'] = user_id
    df_thread['imported_timestamp'] = imported_timestamp
    df_thread['messages'] = [thread_response['messages']]

    return df_thread


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def iter_gmail_history(service: Resource, user_id: str, start_id: str) -> Iterable[dict]:
    response = service.users().history().list(userId=user_id, startHistoryId=start_id).execute()
    if 'history' in response:
        yield from response['history']
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = service.users().history().list(
                                                userId=user_id,
                                                startHistoryId=start_id,
                                                pageToken=page_token
                                            ).execute()
        yield from response['history']


def get_dataframe_for_history_iterable(
        history_iterable: Iterable[dict],
        user_id: str,
        imported_timestamp: str) -> pd.DataFrame:
    df_hist = pd.DataFrame()
    df_temp = pd.DataFrame(history_iterable)
    if 'messages' in df_temp:
        df_temp = df_temp.explode('messages')
        df_hist['historyId'] = df_temp['id']
        df_hist['messages'] = df_temp['messages']
        df_hist['threadId'] = df_hist['messages'].apply(pd.Series)['threadId']
        df_hist['user_id'] = user_id
        df_hist['imported_timestamp'] = imported_timestamp
    else:
        LOGGER.info('No updates found! Response: %s', df_temp)

    return df_hist


def get_gmail_history_details(
        service: Resource,
        user_id: str,
        start_id: str,
        is_gmail_end2end_test: bool) -> pd.DataFrame:
    imported_timestamp = get_current_timestamp()
    history_iterable = iter_gmail_history(service, user_id, start_id)
    if is_gmail_end2end_test:
        history_iterable = islice(history_iterable, 4)
    return get_dataframe_for_history_iterable(history_iterable, user_id, imported_timestamp)


def write_dataframe_to_jsonl_file(
        df_data_to_write: pd.DataFrame,
        target_file_path: str):

    with open(target_file_path, 'w', encoding='utf-8') as file:
        # newline-delimited JSON
        df_data_to_write.to_json(file, orient='records', lines=True, force_ascii=False)


def dataframe_chunk(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos:pos + size]
