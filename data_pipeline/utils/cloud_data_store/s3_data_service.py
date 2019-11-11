"""
s3 data service
written by tayowonibi
"""
import json
import yaml
import boto3
from botocore.exceptions import ClientError


def download_s3_yaml_object_as_json(bucket: str, object_key: str):
    """
    :param bucket:
    :param object_key:
    :return:
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content_as_json = yaml.safe_load(response["Body"])
        return file_content_as_json
    except ClientError:
        return None


def download_s3_json_object(bucket: str, object_key: str):
    """
    :param bucket:
    :param object_key:
    :return:
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        json_obj = json.load(response["Body"])
        return json_obj
    except ClientError:
        return None


def download_s3_object(bucket: str, object_key: str):
    """
    :param bucket:
    :param object_key:
    :return:
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content = response["Body"].read()
        return file_content.decode('utf-8')
    except ClientError:
        return None


def upload_s3_object(bucket: str, object_key: str, data_object):
    """
    :param bucket:
    :param object_key:
    :param data_object:
    :return:
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=data_object, Bucket=bucket, Key=object_key)
        return True
    except ClientError:
        return False
