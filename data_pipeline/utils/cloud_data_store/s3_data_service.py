import logging
import boto3
from botocore.exceptions import ClientError
import pickle
import yaml
import json

def upload_file(file_name, bucket, object_key=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_key is None:
        object_key = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_key)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_s3_yaml_object_as_json(bucket: str, object_key:str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content_as_json = yaml.safe_load(response["Body"])
        return file_content_as_json
    except:
        return None

def download_s3_json_object(bucket: str, object_key:str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        json_obj = json.load(response["Body"])
        return json_obj
    except:
        return None

def download_s3_object(bucket: str, object_key:str):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_content = response["Body"].read()
        return file_content.decode('utf-8')
    except:
        return None

def upload_s3_object(bucket: str, object_key:str, object):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=object, Bucket=bucket, Key=object_key)
        return True
    except:
        return False