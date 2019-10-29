"""
Primarily written to  . . . .
Writes the data primarily to  . . . . .
@author: mowonibi
"""

import requests
import datetime
from datetime import timezone
from datetime import timedelta
from data_pipeline.utils.cloud_data_store.bq_data_service import load_json_list_into_bq
from data_pipeline.utils.cloud_data_store.s3_data_service import download_s3_object
import re
import json


CROSSREF_DATA_COLLECTION_BEGINNING = '2000-01-01'
CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY = 'timestamp'


def get_date_days_before_as_string(number_of_days_before):
    dtobj = datetime.datetime.now(timezone.utc) - timedelta(number_of_days_before)
    return dtobj.strftime("%Y-%m-%d")


def get_last_run_day_from_cloud_storage(bucket: str, object_key:str, number_of_previous_day_to_process=1):
    try:
        date_as_string = download_s3_object(bucket, object_key)
        pattern = r'^\d\d\d\d-\d\d-\d\d$'
        if date_as_string is not None and len(re.findall(pattern, date_as_string.strip())) == 1:
            dtobj = datetime.datetime.strptime(date_as_string.strip(), "%Y-%m-%d") - timedelta(
                number_of_previous_day_to_process)
            return dtobj.strftime("%Y-%m-%d")
        else:
            return get_date_days_before_as_string(number_of_previous_day_to_process)
    except:
        return CROSSREF_DATA_COLLECTION_BEGINNING


def get_crossref_data_single_pass(base_crossref_url: str, cursor=None, publisher_id: str = '', from_date_collected_as_string: str = '2019-08-01', message_key:str='message'):

    url = base_crossref_url + '&from-collected-date=' + from_date_collected_as_string + '&obj-id.prefix=' + publisher_id
    if cursor:
        url += "&cursor=" + cursor
    #r = requests.get(url)
    s = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=5)
    b = requests.adapters.HTTPAdapter(max_retries=5)
    s.mount('http://', a)
    s.mount('https://', b)
    r = s.get(url)

    r.raise_for_status()
    resp = r.json()
    return resp[message_key]['next-cursor'], resp


def write_json_to_file(json_list, full_temp_file_location:str, imported_timestamp_key, imported_timestamp):
    with open(full_temp_file_location, 'a') as write_file:
        for record in json_list:
            write_file.write(json.dumps(standardize_keyname_add_imported_timestamp(record, imported_timestamp_key, imported_timestamp)))
            write_file.write('\n')


def standardize_dict_key_names(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = standardize_dict_key_names(v)
        new_dict[k.replace('-', '_')] = v
    return new_dict


def standardize_keyname_add_imported_timestamp(d, imported_timestamp_key, imported_timestamp):
    dx = standardize_dict_key_names(d)
    dx[imported_timestamp_key] = imported_timestamp
    return dx


def etl_data_return_errors(base_crossref_url:str, from_date_collected_as_string:str, publisher_id:str, dataset_name, table_name, message_key, event_key,
                           imported_timestamp_key, imported_timestamp):
    cursor, downloaded_data = get_crossref_data_single_pass(base_crossref_url=base_crossref_url,
                                                            from_date_collected_as_string=from_date_collected_as_string,
                                                            publisher_id=publisher_id, message_key=message_key)
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    latest_collected_record_timestamp = datetime.datetime.strptime("2000-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
    new_result = []
    for record in results:
        n_record = standardize_keyname_add_imported_timestamp(record, imported_timestamp_key, imported_timestamp)
        new_result.append(n_record)
        record_collection_timestamp = datetime.datetime.strptime(n_record.get(CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY), "%Y-%m-%dT%H:%M:%SZ")
        latest_collected_record_timestamp = latest_collected_record_timestamp if latest_collected_record_timestamp > record_collection_timestamp else record_collection_timestamp

    uninserted_row_list, uninserted_row_message_list = load_json_list_into_bq(new_result, dataset_name, table_name)
    while cursor:
        cursor, downloaded_data = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, cursor=cursor,
                                                                publisher_id=publisher_id,
                                                                from_date_collected_as_string=from_date_collected_as_string, message_key=message_key)
        results = downloaded_data.get(message_key, {}).get(event_key, [])
        new_result = []
        for record in results:
            n_record = standardize_keyname_add_imported_timestamp(record, imported_timestamp_key, imported_timestamp)
            new_result.append(n_record)
            record_collection_timestamp = datetime.datetime.strptime(
                n_record.get(CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY), "%Y-%m-%dT%H:%M:%SZ")
            latest_collected_record_timestamp = latest_collected_record_timestamp if latest_collected_record_timestamp > record_collection_timestamp else record_collection_timestamp
        inner_uninserted_row_list, inner_uninserted_row_message_list = load_json_list_into_bq(new_result, dataset_name, table_name)
        uninserted_row_list.extend(inner_uninserted_row_list)
        uninserted_row_message_list.extend(inner_uninserted_row_message_list)
    return uninserted_row_list, uninserted_row_message_list, latest_collected_record_timestamp.strftime("%Y-%m-%d")
