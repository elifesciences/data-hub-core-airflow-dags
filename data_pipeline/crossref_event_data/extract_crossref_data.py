"""
Primarily written to  . . . .
Writes the data primarily to  . . . . .
@author: mowonibi
"""

import requests
import datetime
from datetime import timezone
from datetime import timedelta
from data_pipeline.utils.load.bq_data_service import  load_json_list_into_bq
import re
import json


def get_date_days_before_as_string(number_of_days_before):
    dtobj = datetime.datetime.now(timezone.utc) - timedelta(number_of_days_before)
    return dtobj.strftime("%Y-%m-%d")


def get_last_run_day(file_name, default_number_of_previous_day_to_process=14):
    try :
        with open(file_name, 'r') as date_state:
            date_as_string = date_state.readline()
            pattern = r'^\d\d\d\d-\d\d-\d\d$'
            if len(re.findall(pattern, date_as_string.strip()))==1:
                return date_as_string.strip()
            else:
                return get_date_days_before_as_string(default_number_of_previous_day_to_process)
    except:
        return get_date_days_before_as_string(default_number_of_previous_day_to_process)


def write_today(file_name):
    with open(file_name, 'w') as f:
        f.write(get_date_days_before_as_string(0))


def get_crossref_data_single_pass(base_crossref_url:str, cursor=None, publisher_id:str = '', from_date_collected_as_string: str = '2019-08-01'):

    url = base_crossref_url + '&from-collected-date=' + from_date_collected_as_string + '&obj-id.prefix=' + publisher_id
    print(url)
    if cursor:
        url += "&cursor=" + cursor
    r = requests.get(url)
    r.raise_for_status()
    resp = r.json()
    return resp['message']['next-cursor'], resp


def get_crossref_data(base_crossref_url:str, from_date_collected_as_string:str, publisher_id:str, full_temp_file_location:str):
    cursor, results = get_crossref_data_single_pass(base_crossref_url=base_crossref_url,from_date_collected_as_string=from_date_collected_as_string, publisher_id=publisher_id)
    write_download_to_file(results, full_temp_file_location)
    while cursor:
        cursor, results = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, cursor=cursor, publisher_id=publisher_id, from_date_collected_as_string=from_date_collected_as_string)
        write_download_to_file(results, full_temp_file_location)


def write_download_to_file(downloaded_data, full_temp_file_location:str, message_key:str, event_key:str):
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    with open(full_temp_file_location, 'a') as write_file:
        for record in results:
            write_file.write(json.dumps(standardize_dict_key_names(record)))
            write_file.write('\n')


def write_json_to_file(json_list, full_temp_file_location:str):
    with open(full_temp_file_location, 'a') as write_file:
        for record in json_list:
            write_file.write(json.dumps(standardize_dict_key_names(record)))
            write_file.write('\n')

def standardize_dict_key_names(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = standardize_dict_key_names(v)
        new_dict[k.replace('-', '_')] = v
    return new_dict


def etl_data_return_errors(base_crossref_url:str, from_date_collected_as_string:str, publisher_id:str, dataset_name, table_name, message_key, event_key):
    cursor, downloaded_data = get_crossref_data_single_pass(base_crossref_url=base_crossref_url,
                                                            from_date_collected_as_string=from_date_collected_as_string,
                                                            publisher_id=publisher_id)
    results = downloaded_data.get(message_key, {}).get(event_key, [])
    results = [standardize_dict_key_names(record) for record in results]

    uninserted_row_list, uninserted_row_message_list = load_json_list_into_bq(results, dataset_name, table_name)
    while cursor:
        cursor, downloaded_data = get_crossref_data_single_pass(base_crossref_url=base_crossref_url, cursor=cursor,
                                                                publisher_id=publisher_id,
                                                                from_date_collected_as_string=from_date_collected_as_string)
        results = downloaded_data.get(message_key, {}).get(event_key, [])
        results = [standardize_dict_key_names(record) for record in results]

        inner_uninserted_row_list, inner_uninserted_row_message_list = load_json_list_into_bq(results, dataset_name, table_name)
        uninserted_row_list.extend(inner_uninserted_row_list)
        uninserted_row_message_list.extend(inner_uninserted_row_message_list)
    return uninserted_row_list, uninserted_row_message_list


def main_pipeline():
    last_day = get_last_run_day('/home/michael/last_run')
    etl_data_return_errors(from_date_collected_as_string=last_day, publisher_id='10.7554', dataset_name='de_dev', table_name='crossref_event')

