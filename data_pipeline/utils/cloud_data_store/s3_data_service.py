import boto3
import yaml
import json


def download_s3_yaml_object_as_json(bucket: str, object_key: str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content_as_json = yaml.safe_load(response["Body"])
        return file_content_as_json
    except BaseException:
        return None


def download_s3_json_object(bucket: str, object_key: str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        json_obj = json.load(response["Body"])
        return json_obj
    except BaseException:
        return None


def download_s3_object(bucket: str, object_key: str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content = response["Body"].read()
        return file_content.decode('utf-8')
    except BaseException:
        return None


def upload_s3_object(bucket: str, object_key: str, object):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=object, Bucket=bucket, Key=object_key)
        return True
    except BaseException:
        return False
