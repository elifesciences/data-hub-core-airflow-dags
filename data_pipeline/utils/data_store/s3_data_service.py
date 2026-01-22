from dataclasses import dataclass
from datetime import datetime
import fnmatch
import json
from contextlib import contextmanager
import logging
from tempfile import NamedTemporaryFile
from typing import Iterable, Mapping, Sequence

import botocore
import yaml
import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger(__name__)


@contextmanager
def s3_open_binary_read(bucket: str, object_key: str):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    streaming_body = response["Body"]
    try:
        yield streaming_body
    finally:
        streaming_body.close()


@contextmanager
def s3_open_binary_read_with_temp_file(bucket: str, object_key: str):
    s3_client = boto3.client("s3")
    LOGGER.debug('s3_client: %r', s3_client)
    with NamedTemporaryFile() as temp_fp:
        s3_client.download_fileobj(Bucket=bucket, Key=object_key, Fileobj=temp_fp)
        temp_fp.seek(0)
        yield temp_fp


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
) -> str:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        file_content = streaming_body.read()
        return file_content.decode("utf-8")


def download_s3_object_as_string_or_file_not_found_error(
    bucket: str, object_key: str
) -> str:
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


def get_s3_object_etag(bucket: str, object_key: str) -> str:
    s3_client = boto3.client("s3")
    response_json = s3_client.head_object(Bucket=bucket, Key=object_key)
    return response_json['ETag']


@dataclass(frozen=True)
class FileMetadata:
    bucket: str
    name: str
    last_modified: datetime


def list_objects_with_pattern_and_timestamp(
    s3_client,
    bucket: str,
    pattern: str,
    latest_timestamp: datetime
) -> Sequence[FileMetadata]:
    """
    List objects in S3 matching pattern and modified after latest_timestamp
    """
    prefix = pattern.split('*')[0]
    paginator = s3_client.get_paginator('list_objects_v2')
    matching_objects: list[FileMetadata] = []
    LOGGER.info(
        'listing s3 objects with bucket: %s, pattern: %s and prefix: %s',
        bucket,
        pattern,
        prefix
    )
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified']
                # Check both pattern match and timestamp
                if (fnmatch.fnmatch(key, pattern) and last_modified > latest_timestamp):
                    matching_objects.append(FileMetadata(
                        bucket=bucket,
                        name=key,
                        last_modified=last_modified
                    ))
    except botocore.exceptions.ClientError as err:
        LOGGER.error('Error listing objects with prefix %s: %s', prefix, err)
        raise

    return matching_objects


@dataclass(frozen=True)
class FileMetadataWithObjectPattern:
    file_metadata: FileMetadata
    object_key_pattern: str


def iter_sorted_new_s3_files_to_process(
    obj_pattern_with_latest_dates: Mapping[str, datetime],
    s3_bucket_name: str,
    is_latest_file_only: bool = False
) -> Iterable[FileMetadataWithObjectPattern]:

    s3_client = boto3.client("s3")
    matching_files: dict[str, Sequence[FileMetadata]] = {}
    LOGGER.debug('obj_pattern_with_latest_dates: %s', obj_pattern_with_latest_dates)

    # For each pattern and its timestamp, get matching objects
    for pattern, latest_timestamp in obj_pattern_with_latest_dates.items():
        objects = list_objects_with_pattern_and_timestamp(
            s3_client=s3_client,
            bucket=s3_bucket_name,
            pattern=pattern,
            latest_timestamp=latest_timestamp
        )
        if objects:
            matching_files[pattern] = objects
            LOGGER.info(
                'Found %d new files for pattern %s modified after %s',
                len(objects),
                pattern,
                latest_timestamp.isoformat()
            )

    # Process matching files
    for object_key_pattern, files_list in matching_files.items():
        sorted_files = sorted(
            files_list,
            key=lambda x: x.last_modified
        )
        if is_latest_file_only:
            sorted_files = [sorted_files[-1]]

        for object_index, file_metadata in enumerate(sorted_files):
            object_key = file_metadata.name
            s3_bucket = file_metadata.bucket

            LOGGER.info(
                'processing file (%d / %d): s3://%s/%s',
                1 + object_index,
                len(sorted_files),
                s3_bucket,
                object_key
            )

            yield FileMetadataWithObjectPattern(
                file_metadata=file_metadata,
                object_key_pattern=object_key_pattern
            )
