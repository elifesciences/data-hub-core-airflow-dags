"""
s3 data service
written by m.owonibi
"""
import json
from contextlib import contextmanager
import yaml
import boto3


@contextmanager
def s3_open_binary_read(bucket: str, object_key: str):
    """
    :param bucket:
    :param object_key:
    :return:
    """
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    streaming_body = response['Body']
    yield streaming_body


def download_s3_yaml_object_as_json(bucket: str, object_key: str) -> dict:
    """
    :param bucket:
    :param object_key:
    :return:
    """
    with s3_open_binary_read(bucket=bucket, object_key=object_key)\
            as streaming_body:
        return yaml.safe_load(streaming_body)


def download_s3_json_object(bucket: str, object_key: str) -> dict:
    """
    :param bucket:
    :param object_key:
    :return:
    """
    with s3_open_binary_read(bucket=bucket, object_key=object_key)\
            as streaming_body:
        return json.load(streaming_body)


def download_s3_object_as_string(bucket: str, object_key: str) -> str:
    """
    :param bucket:
    :param object_key:
    :return:
    """
    with s3_open_binary_read(bucket=bucket, object_key=object_key) \
            as streaming_body:
        file_content = streaming_body.read()
        return file_content.decode('utf-8')


def upload_s3_object(bucket: str, object_key: str, data_object) -> bool:
    """
    :param bucket:
    :param object_key:
    :param data_object:
    :return:
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=data_object, Bucket=bucket, Key=object_key)
    return True
