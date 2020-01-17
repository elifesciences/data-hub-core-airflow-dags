import fnmatch
import re

from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from data_pipeline.s3_csv_data.s3_csv_config import S3CsvConfig

DEFAULT_AWS_CONN_ID = "aws_default"


# pylint: disable=abstract-method,too-many-arguments
class S3NewKeySensor(BaseSensorOperator):
    @apply_defaults
    def __init__(
            self,
            state_info_extract_from_config_callable,
            *args,
            aws_conn_id=DEFAULT_AWS_CONN_ID,
            verify=None,
            **kwargs
    ):
        super(S3NewKeySensor, self).__init__(*args, **kwargs)

        self.object_state_info_extract_from_config_callable = (
            state_info_extract_from_config_callable
        )
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def poke(self, context):
        data_conf_dict = context["dag_run"].conf
        data_config = S3CsvConfig(data_conf_dict, "")
        s3_bucket = data_config.s3_bucket_name
        bucket_key_wildcard_pattern_with_latest_date = (
            self.object_state_info_extract_from_config_callable(
                data_config)
        )
        hook = S3HookNewFileMonitor(
            aws_conn_id=self.aws_conn_id, verify=self.verify
        )
        self.log.info("Poking for keys in  s3 bucket")
        return hook.is_new_file_present(
            bucket_key_wildcard_pattern_with_latest_date, s3_bucket
        )


class S3HookNewFileMonitor(S3Hook):
    def is_new_file_present(
            self,
            bucket_key_wildcard_pattern_with_latest_date: dict,
            bucket_name: str,
            delimiter="",
            page_size=None,
            max_items=None,
    ):

        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }
        for (
                bucket_key_pattern,
                latest_file_deposit_datetime,
        ) in bucket_key_wildcard_pattern_with_latest_date.items():
            prefix = re.split(r"[*]", bucket_key_pattern, 1)[0]

            paginator = self.get_conn().get_paginator("list_objects_v2")
            response = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                PaginationConfig=config,
            )
            for page in response:
                if "Contents" in page:
                    for k in page["Contents"]:
                        if (
                                k["LastModified"]
                                > latest_file_deposit_datetime
                                and
                                fnmatch.fnmatch(k["Key"], bucket_key_pattern)
                        ):
                            return True
        return False

    def get_new_object_key_names(
            self,
            bucket_key_wildcard_pattern_with_latest_date: dict,
            bucket_name: str,
            delimiter="",
            page_size=None,
            max_items=None,
    ):

        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }
        new_object_key_names = dict()
        for (
                bucket_key_pattern,
                latest_file_deposit_datetime,
        ) in bucket_key_wildcard_pattern_with_latest_date.items():
            prefix = re.split(r"[*]", bucket_key_pattern, 1)[0]

            paginator = self.get_conn().get_paginator("list_objects_v2")
            response = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                PaginationConfig=config,
            )

            for page in response:
                if "Contents" in page:
                    new_object_key_names[bucket_key_pattern] = [
                        k
                        for k in page["Contents"]
                        if k["LastModified"] > latest_file_deposit_datetime
                        and fnmatch.fnmatch(k["Key"], bucket_key_pattern)
                    ]

        return new_object_key_names
