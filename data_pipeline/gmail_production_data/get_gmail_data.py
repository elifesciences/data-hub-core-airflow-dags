import pandas as pd

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


def write_dataframe_to_file(df_data_to_write, target_file_path):
    '''
    To write the given DataFrame on CSV file

    param:
        - df_data_to_write : pandas DataFrame
        - target_file_path : file path/name to write the data

    return: None
    '''
    df_data_to_write.to_csv(target_file_path, index=False)
