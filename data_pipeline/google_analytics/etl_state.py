from datetime import datetime

from data_pipeline.google_analytics.ga_config import (
    STORED_STATE_FORMAT,
    GoogleAnalyticsConfig,
    parse_date
)
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_object_as_string_or_file_not_found_error,
    upload_s3_object
)


def update_state(
        latest_state_date: datetime,
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    upload_s3_object(
        data_object=latest_state_date.strftime(STORED_STATE_FORMAT),
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object
    )


def get_stored_state_date_or_default_start_date(
        data_config: GoogleAnalyticsConfig
) -> datetime:
    try:
        return parse_date(download_s3_object_as_string_or_file_not_found_error(
            data_config.state_s3_bucket_name,
            data_config.state_s3_object_name
        ))
    except FileNotFoundError:
        return data_config.default_start_date
