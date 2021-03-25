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


@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
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
    imported_timestamp = get_current_timestamp()
    thread_results = service.users().threads().get(userId=user_id, id=thread_id).execute()

    df_thread = pd.DataFrame()

    df_thread['threadId'] = [thread_results['id']]
    df_thread['historyId'] = [thread_results['historyId']]
    df_thread['user_id'] = user_id
    df_thread['imported_timestamp'] = imported_timestamp
    df_thread['messages'] = [thread_results['messages']]

    return df_thread


def write_dataframe_to_jsonl_file(
        df_data_to_write: pd.DataFrame,
        target_file_path: str):

    with open(target_file_path, 'w', encoding='utf-8') as file:
        # newline-delimited JSON
        df_data_to_write.to_json(file, orient='records', lines=True, force_ascii=False)


# pylint: disable=duplicate-string-formatting-argument
@backoff.on_exception(backoff.expo, TimeoutError, max_tries=10)
def get_distinct_values_from_bq(
            project_name: str,
            dataset: str,
            column_name: str,
            table_name_to_execute: str,
            table_name_executed: str
        ) -> pd.DataFrame:

    sql = (
        """
        SELECT DISTINCT {}
        FROM  `{}.{}.{}`
        WHERE {} NOT IN
            (
                SELECT {}
                FROM `{}.{}.{}`
            )
        """.format(
                column_name,
                project_name,
                dataset,
                table_name_to_execute,
                column_name,
                column_name,
                project_name,
                dataset,
                table_name_executed
            )
    )

    client = bigquery.Client(project_name)
    query_job = client.query(sql)  # API request
    results = query_job.result()  # Waits for query to finish

    return pd.concat([pd.DataFrame([row.threadId]) for row in results], ignore_index=True)


def dataframe_chunk(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos:pos + size]
