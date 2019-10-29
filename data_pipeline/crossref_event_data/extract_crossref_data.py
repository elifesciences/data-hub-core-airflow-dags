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


def get_date_days_before_as_string(number_of_days_before):
    dtobj = datetime.datetime.now(timezone.utc) - timedelta(number_of_days_before)
    return dtobj.strftime("%Y-%m-%d")


def get_last_run_day_from_file(file_name, default_number_of_previous_day_to_process=14):
    print(23)
    try:
        with open(file_name, 'r') as date_state:
            date_as_string = date_state.readline()
            pattern = r'^\d\d\d\d-\d\d-\d\d$'
            if len(re.findall(pattern, date_as_string.strip()))==1:
                return date_as_string.strip()
            else:
                return get_date_days_before_as_string(default_number_of_previous_day_to_process)
    except:
        return get_date_days_before_as_string(default_number_of_previous_day_to_process)


def get_last_run_day_from_cloud_storage(bucket: str, object_key:str, default_number_of_previous_day_to_process=14):
    try:
        date_as_string = download_s3_object(bucket, object_key)
        print(date_as_string)
        pattern = r'^\d\d\d\d-\d\d-\d\d$'
        if date_as_string is not None and len(re.findall(pattern, date_as_string.strip())) == 1:
            return date_as_string.strip()
        else:
            return "2323"
            #return get_date_days_before_as_string(default_number_of_previous_day_to_process)
    except:
        return 'reospfs'
        #return get_date_days_before_as_string(default_number_of_previous_day_to_process)



def write_today(file_name):
    with open(file_name, 'w') as f:
        f.write(get_date_days_before_as_string(0))


def get_crossref_data_single_pass(base_crossref_url:str, cursor=None, publisher_id:str = '', from_date_collected_as_string: str = '2019-08-01'):

    url = base_crossref_url + '&from-collected-date=' + from_date_collected_as_string + '&obj-id.prefix=' + publisher_id
    if cursor:
        url += "&cursor=" + cursor
    r = requests.get(url)
    r.raise_for_status()
    resp = r.json()
    return resp['message']['next-cursor'], resp


def get_crossref_data(base_crossref_url:str, from_date_collected_as_string:str, publisher_id:str, message_key:str, event_key:str, full_temp_file_location:str, imported_timestamp_key, imported_timestamp):
    cursor, results = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, from_date_collected_as_string=from_date_collected_as_string, publisher_id=publisher_id)
    write_download_to_file(results, full_temp_file_location, message_key=message_key, event_key=event_key,  imported_timestamp_key=imported_timestamp_key, imported_timestamp=imported_timestamp)
    while cursor:
        cursor, results = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, cursor=cursor, publisher_id=publisher_id, from_date_collected_as_string=from_date_collected_as_string)
        write_download_to_file(results, full_temp_file_location, message_key=message_key, event_key=event_key,imported_timestamp_key=imported_timestamp_key, imported_timestamp=imported_timestamp)


def write_download_to_file(downloaded_data, full_temp_file_location:str, message_key:str, event_key:str, imported_timestamp_key, imported_timestamp):
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    with open(full_temp_file_location, 'a') as write_file:
        for record in results:
            write_file.write(json.dumps(standardize_keyname_add_imported_timestamp(record,imported_timestamp_key, imported_timestamp)))
            write_file.write('\n')


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
                                                            publisher_id=publisher_id)
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    results = [standardize_keyname_add_imported_timestamp(record, imported_timestamp_key, imported_timestamp) for record  in results]

    uninserted_row_list, uninserted_row_message_list = load_json_list_into_bq(results, dataset_name, table_name)
    while cursor:
        cursor, downloaded_data = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, cursor=cursor,
                                                                publisher_id=publisher_id,
                                                                from_date_collected_as_string=from_date_collected_as_string)
        results = downloaded_data.get(message_key, {}).get(event_key, [])
        results = [standardize_keyname_add_imported_timestamp(record, imported_timestamp_key, imported_timestamp) for record  in results]

        inner_uninserted_row_list, inner_uninserted_row_message_list = load_json_list_into_bq(results, dataset_name, table_name)
        uninserted_row_list.extend(inner_uninserted_row_list)
        uninserted_row_message_list.extend(inner_uninserted_row_message_list)
    return uninserted_row_list, uninserted_row_message_list
