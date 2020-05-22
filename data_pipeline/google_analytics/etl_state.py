import json
from datetime import datetime
from botocore.exceptions import ClientError
from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig
from data_pipeline.utils.data_store.s3_data_service import (
    upload_s3_object, download_s3_object_as_string
)


STORED_STATE_FORMAT = '%Y-%m-%d'


def update_state(
        latest_state_date: datetime,
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    upload_s3_object(
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object,
        data_object=latest_state_date.strftime(STORED_STATE_FORMAT)
    )


def get_stored_state(
        data_config: GoogleAnalyticsConfig,
        default_latest_state_date: str = None,
) -> str:
    try:
        stored_state = download_s3_object_as_string(
            data_config.state_s3_bucket_name,
            data_config.state_s3_object_name
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = (
                    default_latest_state_date or
                    data_config.default_start_date_as_string
            )
        else:
            raise ex
    return stored_state