import json
import os
from contextlib import contextmanager
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from typing import Iterable

import fsspec

import yaml


def iter_write_jsonl_to_file(
        json_list,
        full_temp_file_location: str,
        write_mode: str = 'a'
) -> Iterable[dict]:
    with open(full_temp_file_location, write_mode, encoding='UTF-8') as write_file:
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
    with open(full_temp_file_location, write_mode, encoding='UTF-8') as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
            write_file.write("\n")
        write_file.flush()


def read_file_content(file_location: str):
    with open(file_location, 'r', encoding='UTF-8') as open_file:
        data = open_file.read()
    return data


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r', encoding='UTF-8') as yaml_file:
        return yaml.safe_load(yaml_file)


def is_remote_path(urlpath: str) -> bool:
    return '://' in str(urlpath)


def download_file(urlpath: str, local_path: str):
    # Note: it might be more efficient to ask fsspec to download the file
    with open(local_path, 'wb') as local_fp:
        with fsspec.open(urlpath, mode='rb') as remote_fp:
            copyfileobj(remote_fp, local_fp)


@contextmanager
def get_temp_local_file_if_remote(urlpath: str) -> str:
    if not is_remote_path(urlpath):
        yield urlpath
        return
    with TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, os.path.basename(urlpath))
        download_file(urlpath, local_path)
        yield local_path
