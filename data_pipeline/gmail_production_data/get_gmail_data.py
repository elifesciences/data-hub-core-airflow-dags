from typing import Iterable
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource


# this connection type will be changed !!
def connect_to_email(userId: str) -> Resource:
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    SERVICE_ACCOUNT_SECRET_FILE = 'DELETE/elife-data-pipeline-test.json'
    EMAIL_ADDRESS = userId

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_SECRET_FILE, scopes=SCOPES)
    delegated_credentials = credentials.with_subject(EMAIL_ADDRESS)

    service = build("gmail", "v1", credentials=delegated_credentials)
    return service


def get_label_list(service: Resource, userId: str) -> pd.DataFrame:
    label_response = service.users().labels().list(userId=userId).execute()
    df = pd.DataFrame(label_response['labels'])
    return df


def iter_link_message_thread(service: Resource, userId: str) -> Iterable[dict]:
    response = service.users().messages().list(userId=userId).execute()
    if 'messages' in response:
        yield from response['messages']
    while ('nextPageToken' in response):
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId=userId, pageToken=page_token).execute()
        yield from response['messages']


def get_link_message_thread(service: Resource, userId: str) -> pd.DataFrame:
    df = pd.DataFrame(iter_link_message_thread(service, userId))
    return df


def write_dataframe_to_file(df_data_to_write: pd.DataFrame, target_file_path: str):
    df_data_to_write.to_csv(target_file_path, encoding='utf-8', index=False)
