import boto3
import dateutil.parser
import logging
import pytz
from datetime import  datetime

logger = logging.getLogger(__name__)


def get_s3_objects(bucket, prefixes, last_modified_date: datetime):
    prefixes = set(prefixes)
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    for prefix in prefixes:
        kwargs['Prefix'] = prefix
        while True:
            resp = s3.list_objects_v2(**kwargs)
            for content in resp.get('Contents', []):
                object_last_modified_date = content['LastModified']
                if object_last_modified_date > last_modified_date:
                    yield content

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break


def is_new_object_present(bucket, prefixes, last_modified_date: datetime):
    prefixes = set(prefixes)
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    for prefix in prefixes:
        kwargs['Prefix'] = prefix
        while True:
            resp = s3.list_objects_v2(**kwargs)
            for content in resp.get('Contents', []):
                object_last_modified_date = content['LastModified']
                if object_last_modified_date > last_modified_date:
                    return True

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
    return False


def get_s3_keys(bucket, prefixes=None, last_modified_date=None):

    for obj in get_s3_objects(bucket, prefixes, last_modified_date):
        yield obj['Key']


def valid_datetime(date):
    if date is None:
        return date
    utc = pytz.UTC
    return utc.localize(dateutil.parser.parse(date))


#get_s3_keys(args.bucket, args.prefixes, args.suffixes, args.last_modified_min, args.last_modified_max)
