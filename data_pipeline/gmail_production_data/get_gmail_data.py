import pandas as pd
import numpy as np

from google.oauth2 import service_account
from googleapiclient.discovery import build


# this connection type will be changed !!
def connect_to_email(userId):
    '''
    To connect gmail account via service account
    param:
        - userId : email adress
    return: None
    '''
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    SERVICE_ACCOUNT_FILE = 'DELETE/elife-data-pipeline-test.json'
    EMAIL_ADDRESS = userId

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    delegated_credentials = credentials.with_subject(EMAIL_ADDRESS)

    service = build("gmail", "v1", credentials=delegated_credentials)
    return service


def get_label_list(service, userId):
    '''
    To get list of label ids which have created by system or by user
    And to write the information on CSV file
    param:
        - userId : str
    return: pandas DafaFrame
    '''
    label_response = service.users().labels().list(userId=userId).execute()

    df = pd.DataFrame(label_response['labels'])

    return df


def get_necessary_label_list(df_label):
    '''
    To select the necessary labelIds to filter the data by just these labels

    param:
        - df_label : pandas DataFrame

    return:
        - LABELS_NEED : list
    '''
    label_name = 'Hiver-production'
    df_label = df_label[df_label['name'].str.startswith(label_name)]

    LABELS_NEED = df_label['id'].values.tolist()

    return LABELS_NEED


def get_link_message_thread(service, userId, labels_need):
    '''
    To get relations of all message and thread ids for specific label list

    param:
        - service : connection
        - userId : str
        - label : list
    return:
        - df : DataFrame
    '''
    message_list = []
    pages = 0
    limit = 10000
    # get emails for specific labels
    for labelId in labels_need:
        pages = 0
        response = service.users().messages().list(userId=userId, labelIds=labelId).execute()
        if 'messages' in response:
            #  to add the end of the list
            message_list.extend(response['messages'])
        while ('nextPageToken' in response) and (pages < limit):
            page_token = response['nextPageToken']
            response = service.users().messages().list(
                userId=userId,
                labelIds=labelId,
                pageToken=page_token
                ).execute()
            message_list.extend(response['messages'])
            pages = pages + 1

    # to get unique list of dict list
    list_of_unique_dicts = list(np.unique(np.array(message_list).astype(str)))
    df = pd.DataFrame(list_of_unique_dicts).replace(
        "{'id': '", "", regex=True
        ).replace(
            "', 'threadId': '", ",", regex=True
            ).replace("'}", "", regex=True)
    df[['messageId', 'threadId']] = df[0].str.split(',', 1, expand=True)

    return df[['messageId', 'threadId']]


def write_dataframe_to_file(df_data_to_write, target_file_path):
    '''
    To write the given DataFrame on CSV file

    param:
        - df_data_to_write : pandas DataFrame
        - target_file_path : file path/name to write the data

    return: None
    '''
    df_data_to_write.to_csv(target_file_path, index=False)
