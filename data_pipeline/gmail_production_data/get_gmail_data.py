import pandas as pd
import time
from functools import wraps

from google.oauth2 import service_account
from googleapiclient.discovery import build


# this connection type will be changed !!
def connect_to_email(userId):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    SERVICE_ACCOUNT_FILE = 'DELETE/elife-data-pipeline-test.json'
    EMAIL_ADDRESS = userId

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    delegated_credentials = credentials.with_subject(EMAIL_ADDRESS)

    service = build("gmail", "v1", credentials=delegated_credentials)
    return service


def get_label_list(service, userId):
    label_response = service.users().labels().list(userId=userId).execute()
    df = pd.DataFrame(label_response['labels'])
    return df


def iter_link_message_thread(service, userId):
    response = service.users().messages().list(userId=userId).execute()
    if 'messages' in response:
        yield from response['messages']
    while ('nextPageToken' in response):
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId=userId, pageToken=page_token).execute()
        yield from response['messages']


def get_link_message_thread(service, userId):
    df = pd.DataFrame(iter_link_message_thread(service, userId))
    return df


def write_dataframe_to_file(df_data_to_write, target_file_path):
    df_data_to_write.to_csv(target_file_path, encoding='utf-8-sig', index=False)
