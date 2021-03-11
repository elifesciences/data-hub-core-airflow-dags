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


def retry(ExceptionToCheck, tries=5, delay=4, backoff=3, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(Exception, tries=5)
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


@retry(Exception, tries=5)
def get_one_thread(service, userId, threadId):
    """
    To get thread details like :
        - threadId
        - historyId
        - total_email_count
        - sent_email_count
        - first_responder
        - first_reponse_date
    """
    thread_results = service.users().threads().get(userId=userId, id=threadId).execute()

    df = pd.DataFrame()

    df['threadId'] = [thread_results['id']]
    df['historyId'] = [thread_results['historyId']]
    df['total_email_count'] = [len(thread_results['messages'])]

    # to find count of emails from elifesciences.org in a thread (sent_email_count)
    sent_email_count = 0
    EMAIL_SENT_PATTERN = 'elifesciences.org'
    for i in range(len(thread_results['messages'])):
        header_list = thread_results['messages'][i]['payload']['headers']
        for j in range(len(header_list)):
            if header_list[j]['name'] == 'From':
                if EMAIL_SENT_PATTERN in header_list[j]['value']:
                    sent_email_count += 1

    df['sent_email_count'] = sent_email_count

    # to get first responder
    FISRT_RESPONSE_ITERATION = 0
    for i in range(len(thread_results['messages'])):
        header_list = thread_results['messages'][i]['payload']['headers']
        for j in range(len(header_list)):
            if header_list[j]['name'] == 'From':
                if 'elifesciences.org' in header_list[j]['value']:
                    df['first_responder'] = [header_list[j]['value']]
                    FISRT_RESPONSE_ITERATION = i
                    break

    # to get first response date
    header_list = thread_results['messages'][FISRT_RESPONSE_ITERATION]['payload']['headers']
    for k in range(len(header_list)):
        if header_list[k]['name'] == 'Date':
            df['first_reponse_date'] = [header_list[k]['value']]
            break

    return df


def get_thread_details_for_given_ids(service, userId, threadIds_file):
    df = pd.read_csv(threadIds_file, usecols=['threadId'])
    threadIds_arr = df['threadId'].unique()
    df = pd.DataFrame()
    for threadId in threadIds_arr:
        df_one_thread = get_one_thread(service, userId, threadId)
        df = df.append(df_one_thread)

    return df


def write_dataframe_to_file(df_data_to_write, target_file_path):
    df_data_to_write.to_csv(target_file_path, encoding='utf-8-sig', index=False)
