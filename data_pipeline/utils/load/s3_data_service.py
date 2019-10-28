import logging
import boto3
from botocore.exceptions import ClientError
import pickle



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

def upload_serialized_in_memory_object(serialized_object, bucket, object_key):
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=object_key, Body=serialized_object)
    except ClientError as e:
        logging.error(e)
        return False
    return True
