from typing import Iterable
import json

import yaml


def iter_write_jsonl_to_file(
        json_list,
        full_temp_file_location: str,
        write_mode: str = 'a'
) -> Iterable[dict]:
    with open(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
            write_file.write("\n")
            yield record
        write_file.flush()


def write_jsonl_to_file(
        json_list: Iterable,
        full_temp_file_location: str,
        write_mode: str = 'w'
):
    with open(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
            write_file.write("\n")
        write_file.flush()


def read_file_content(file_location: str):
    with open(file_location, 'r') as open_file:
        data = open_file.read()
    return data


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)
