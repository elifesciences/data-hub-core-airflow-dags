import json
import re
from botocore.exceptions import ClientError
from data_pipeline.utils.data_store.s3_data_service import (
    upload_s3_object,
    download_s3_yaml_object_as_json
)

STORED_STATE_FORMAT = '%Y-%m-%d'


def get_s3_object_name_for_search_term(statefile_s3_object, search_term):
    return '/'.join([statefile_s3_object, re.sub("[^\\w]", "", search_term)])


def update_state(
        latest_status_id: str,
        latest_state_date: str,
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    multi_object_stored_state = get_stored_state(
        statefile_s3_bucket, statefile_s3_object
    )
    multi_object_stored_state = {
        **multi_object_stored_state,
        latest_status_id: latest_state_date
    }
    upload_s3_object(
        data_object=json.dumps(multi_object_stored_state),
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object
    )


def get_stored_state_for_object_id(
        statefile_s3_bucket: str,
        statefile_s3_object: str,
        object_id: str,
        default_latest_state_date: str = None,
) -> str:
    multi_object_stored_state = get_stored_state(
        statefile_s3_bucket, statefile_s3_object
    ) or {}
    return multi_object_stored_state.get(
        object_id, default_latest_state_date
    )


def get_stored_state(
        statefile_s3_bucket: str,
        statefile_s3_object: str,
) -> dict:
    try:
        stored_state = download_s3_yaml_object_as_json(
            statefile_s3_bucket,
            statefile_s3_object
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = {}
        else:
            raise ex
    return stored_state
