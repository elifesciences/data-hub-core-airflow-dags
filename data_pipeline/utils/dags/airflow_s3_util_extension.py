import fnmatch
import re
from collections import defaultdict

from typing import Iterable

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from data_pipeline.s3_csv_data.s3_csv_config import S3BaseCsvConfig
from data_pipeline.s3_csv_data.s3_csv_etl import NamedLiterals


# pylint: disable=abstract-method,too-many-arguments
class S3NewKeyFromLastDataDownloadDateSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(
            self,
            state_info_extract_from_config_callable,
            default_initial_s3_last_modified_date,
            deployment_environment,
            *args,
            aws_conn_id=NamedLiterals.DEFAULT_AWS_CONN_ID,
            verify=None,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.object_state_info_extract_from_config_callable = (
            state_info_extract_from_config_callable
        )
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.default_initial_s3_last_modified_date = (
            default_initial_s3_last_modified_date
        )
        self.deployment_environment = deployment_environment

    def poke(self, context):
        data_conf_dict = context["dag_run"].conf
        data_config = S3BaseCsvConfig(
            data_conf_dict,
            self.deployment_environment
        )

        s3_bucket = data_config.s3_bucket_name
        bucket_key_wildcard_pattern_with_latest_date = (
            self.object_state_info_extract_from_config_callable(
                data_config,
                self.default_initial_s3_last_modified_date)
        )
        hook = S3HookNewFileMonitor(
            aws_conn_id=self.aws_conn_id, verify=self.verify
        )
        self.log.info("Poking for keys in s3 bucket")
        return hook.is_new_file_present(
            bucket_key_wildcard_pattern_with_latest_date, s3_bucket
        )


class S3HookNewFileMonitor(S3Hook):
    def is_new_file_present(
            self,
            bucket_key_wildcard_pattern_with_latest_date: dict,
            bucket_name: str,
    ):
        object_key_names_iter = self.iter_filter_s3_object_meta_after(
            bucket_key_wildcard_pattern_with_latest_date,
            bucket_name
        )
        for _ in object_key_names_iter:
            return True

        return False

    def get_new_object_key_names(
            self,
            bucket_key_wildcard_pattern_with_latest_date: dict,
            bucket_name: str
    ):
        new_object_key_names = defaultdict(list)
        object_key_names_iter = self.iter_filter_s3_object_meta_after(
            bucket_key_wildcard_pattern_with_latest_date,
            bucket_name
        )
        for bucket_key_pattern, key_object in object_key_names_iter:
            new_object_key_names[bucket_key_pattern].append(key_object)

        return new_object_key_names

    def iter_filter_s3_object_meta_after(
            self,
            bucket_key_wildcard_pattern_with_latest_date: dict,
            bucket_name: str,
            delimiter="",
            page_size=None,
            max_items=None,
    ) -> Iterable[dict]:

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
                    for key_object in page["Contents"]:
                        if (
                                key_object["LastModified"]
                                > latest_file_deposit_datetime
                                and
                                fnmatch.fnmatch(
                                    key_object["Key"], bucket_key_pattern
                                )
                        ):
                            yield bucket_key_pattern, key_object
