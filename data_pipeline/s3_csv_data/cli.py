import argparse
import logging
import os
from typing import Optional, Sequence

from data_pipeline.s3_csv_data.s3_csv_config import (
    DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE,
    MultiS3CsvConfig,
    S3BaseCsvConfig
)
from data_pipeline.s3_csv_data.s3_csv_etl import (
    get_stored_state,
    transform_load_data,
    update_object_latest_dates,
    upload_s3_object_json
)
from data_pipeline.utils.data_pipeline_timestamp import get_current_timestamp_as_string
from data_pipeline.utils.data_store.s3_data_service import iter_sorted_new_s3_files_to_process
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class S3CsvEnvironmentVariables:
    CONFIG_FILE_PATH = 'S3_CSV_CONFIG_FILE_PATH'
    INITIAL_S3_FILE_LAST_MODIFIED_DATE = 'INITIAL_S3_FILE_LAST_MODIFIED_DATE'


def get_default_initial_s3_last_modified_date():
    return os.getenv(
        S3CsvEnvironmentVariables.INITIAL_S3_FILE_LAST_MODIFIED_DATE,
        DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
    )


def get_multi_csv_pipeline_config() -> MultiS3CsvConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        S3CsvEnvironmentVariables.CONFIG_FILE_PATH,
        MultiS3CsvConfig
    )


def etl_new_csv_files(data_config: S3BaseCsvConfig):
    obj_pattern_with_latest_dates = get_stored_state(
        data_config,
        get_default_initial_s3_last_modified_date()
    )
    new_s3_files = list(iter_sorted_new_s3_files_to_process(
        obj_pattern_with_latest_dates=obj_pattern_with_latest_dates,
        s3_bucket_name=data_config.s3_bucket_name
    ))
    if not new_s3_files:
        LOGGER.info('No new file found and skipped the task.')
        return
    for matching_file_metadata_with_object_pattern in new_s3_files:
        record_import_timestamp_as_string = get_current_timestamp_as_string()
        matching_file_metadata = matching_file_metadata_with_object_pattern.file_metadata
        object_key_pattern = matching_file_metadata_with_object_pattern.object_key_pattern
        transform_load_data(
            matching_file_metadata.name,
            data_config,
            record_import_timestamp_as_string,
        )
        updated_obj_pattern_with_latest_dates = (
            update_object_latest_dates(
                obj_pattern_with_latest_dates,
                object_key_pattern,
                matching_file_metadata.last_modified
            )
        )
        upload_s3_object_json(
            updated_obj_pattern_with_latest_dates,
            data_config.state_file_bucket_name,
            data_config.state_file_object_name
        )


def csv_etl(data_pipeline_id: str, **_kwargs):
    multi_csv_pipeline_config = get_multi_csv_pipeline_config()
    data_config_dict = multi_csv_pipeline_config.s3_csv_config_dict_by_pipeline_id[data_pipeline_id]
    data_config = S3BaseCsvConfig(data_config_dict)
    etl_new_csv_files(data_config=data_config)


def main(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description='Run ETL for a specific S3 CSV pipeline')
    parser.add_argument('--data-pipeline-id', required=True)
    args = parser.parse_args(argv)
    data_pipeline_id = args.data_pipeline_id
    LOGGER.info('Starting ETL for pipeline: %s', data_pipeline_id)
    csv_etl(data_pipeline_id)
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
