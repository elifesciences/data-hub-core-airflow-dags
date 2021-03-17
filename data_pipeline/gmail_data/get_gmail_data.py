from typing import Iterable
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource


def get_gmail_service_for_user_id(secret_file: str, scopes: str, user_id: str) -> Resource:
    credentials = service_account.Credentials.from_service_account_file(
        secret_file, scopes=scopes)
    delegated_credentials = credentials.with_subject(user_id)

    service = build("gmail", "v1", credentials=delegated_credentials)
    return service


def get_label_list(service: Resource, user_id: str) -> pd.DataFrame:
    label_response = service.users().labels().list(userId=user_id).execute()

    df = pd.DataFrame(label_response['labels'])
    return df


def iter_link_message_thread(service: Resource, user_id: str) -> Iterable[dict]:
    response = service.users().messages().list(userId=user_id).execute()
    if 'messages' in response:
        yield from response['messages']
    while ('nextPageToken' in response):
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId=user_id, pageToken=page_token).execute()
        yield from response['messages']


def get_link_message_thread(service: Resource, user_id: str) -> pd.DataFrame:
    df = pd.DataFrame(iter_link_message_thread(service, user_id))
    return df


def write_dataframe_to_file(
        df_data_to_write: pd.DataFrame,
        target_file_path: str,
        string_file_name: str):

    if string_file_name.lower().endswith('.csv'):
        df_data_to_write.to_csv(target_file_path, encoding='utf-8', index=False)
    elif string_file_name.lower().endswith('.json'):
        with open(target_file_path, 'w', encoding='utf-8') as file:
            # newline-delimited JSON
            df_data_to_write.to_json(file, orient='records', lines=True, force_ascii=False)
