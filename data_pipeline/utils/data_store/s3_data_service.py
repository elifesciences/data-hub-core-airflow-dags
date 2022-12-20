import json
from contextlib import contextmanager

import yaml
import boto3
from botocore.exceptions import ClientError


@contextmanager
def s3_open_binary_read(bucket: str, object_key: str):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    streaming_body = response["Body"]
    try:
        yield streaming_body
    finally:
        streaming_body.close()


def download_s3_yaml_object_as_json(bucket: str, object_key: str) -> dict:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        return yaml.safe_load(streaming_body)


def download_s3_json_object(bucket: str, object_key: str) -> dict:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        return json.load(streaming_body)


def download_s3_object_as_string(
        bucket: str, object_key: str
) -> dict:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        file_content = streaming_body.read()
        return file_content.decode("utf-8")


def download_s3_object_as_string_or_file_not_found_error(
    bucket: str, object_key: str
) -> dict:
    try:
        return download_s3_object_as_string(bucket, object_key)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            raise FileNotFoundError(str(ex)) from ex
        raise


def upload_s3_object(bucket: str, object_key: str, data_object) -> bool:
    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data_object, Bucket=bucket, Key=object_key)
    return True


def delete_s3_object(bucket, object_key):
    s3_client = boto3.client('s3')
    s3_client.delete_object(
        Bucket=bucket,
        Key=object_key
    )
