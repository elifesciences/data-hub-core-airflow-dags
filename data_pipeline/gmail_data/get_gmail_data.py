import datetime
from typing import Iterable
import pandas as pd
import backoff

from google.oauth2 import service_account
from google.cloud import bigquery

from googleapiclient.discovery import build
from googleapiclient.discovery import Resource


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


def iter_link_message_thread_ids(service: Resource, user_id: str) -> Iterable[dict]:
    response = service.users().messages().list(userId=user_id).execute()
    if 'messages' in response:
        yield from response['messages']
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = service.users().messages().list(userId=user_id, pageToken=page_token).execute()
        yield from response['messages']


def get_link_message_thread_ids(service: Resource, user_id: str) -> pd.DataFrame:
    imported_timestamp = get_current_timestamp()
    df_link = pd.DataFrame(iter_link_message_thread_ids(service, user_id))
    df_link['user_id'] = user_id
    df_link['imported_timestamp'] = imported_timestamp
    return df_link


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def get_one_thread(service: str, user_id: str, thread_id: str) -> pd.DataFrame:

    thread_results = service.users().threads().get(userId=user_id, id=thread_id).execute()

    df_thread = pd.DataFrame()
    messages = thread_results['messages']

    df_thread['threadId'] = [thread_results['id']]
    df_thread['historyId'] = [thread_results['historyId']]
    df_thread['total_email_count'] = [len(messages)]

    # to find count of emails from a specific address in a thread (sent_email_count)
    sent_email_count = 0
    email_sent_pattern = 'elifesciences.org'
    for message in messages:
        header_list = message['payload']['headers']
        for header in header_list:
            if header['name'] == 'From':
                if email_sent_pattern in header['value']:
                    sent_email_count += 1

    df_thread['sent_email_count'] = sent_email_count

    # to get first responder
    # pylint: disable=consider-using-enumerate
    first_response_index = 0
    for i in range(len(messages)):
        header_list = messages[i]['payload']['headers']
        for header in header_list:
            if header['name'] == 'From':
                if email_sent_pattern in header['value']:
                    df_thread['first_responder'] = [header['value']]
                    first_response_index = i
                    break

    # to get first response date
    header_list = messages[first_response_index]['payload']['headers']
    for header in header_list:
        if header['name'] == 'Date':
            df_thread['first_reponse_date'] = [header['value']]
            break

    return df_thread


def write_dataframe_to_jsonl_file(
        df_data_to_write: pd.DataFrame,
        target_file_path: str):

    with open(target_file_path, 'w', encoding='utf-8') as file:
        # newline-delimited JSON
        df_data_to_write.to_json(file, orient='records', lines=True, force_ascii=False)


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def get_distinct_values_from_bq(
            project_name: str,
            dataset: str,
            column_name: str,
            table_name: str
        ) -> pd.DataFrame:

    sql = (
        """
        SELECT DISTINCT {}
        FROM  `{}.{}.{}`
        """.format(column_name, project_name, dataset, table_name)
    )

    client = bigquery.Client(project_name)
    query_job = client.query(sql)  # API request
    results = query_job.result()  # Waits for query to finish

    df_value = pd.DataFrame()
    df_temp = pd.DataFrame()
    for row in results:
        df_temp[column_name] = [row.threadId]
        df_value = pd.concat([df_value, df_temp], ignore_index=True)

    return df_value
