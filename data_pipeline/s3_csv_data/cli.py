import argparse
import logging
from dags.s3_csv_import_pipeline import get_default_initial_s3_last_modified_date
from data_pipeline.s3_csv_data.s3_csv_config import (
    MultiS3CsvConfig,
    S3BaseCsvConfig
)
from data_pipeline.s3_csv_data.s3_csv_etl import (
    NamedLiterals,
    get_stored_state,
    transform_load_data,
    update_object_latest_dates,
    upload_s3_object_json
)
from data_pipeline.utils.dags.airflow_s3_util_extension import S3HookNewFileMonitor
from data_pipeline.utils.data_pipeline_timestamp import get_current_timestamp_as_string
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class S3CsvEnvironmentVariables:
    CONFIG_FILE_PATH = 'S3_CSV_CONFIG_FILE_PATH'


def get_multi_csv_pipeline_config() -> MultiS3CsvConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        S3CsvEnvironmentVariables.CONFIG_FILE_PATH,
        MultiS3CsvConfig
    )


def etl_new_csv_files(data_config: S3BaseCsvConfig):
    obj_pattern_with_latest_dates = (
        get_stored_state(
            data_config,
            get_default_initial_s3_last_modified_date()
        )
    )
    hook = S3HookNewFileMonitor(
        aws_conn_id=NamedLiterals.DEFAULT_AWS_CONN_ID,
        verify=None
    )
    new_s3_files = hook.get_new_object_key_names(
        obj_pattern_with_latest_dates,
        data_config.s3_bucket_name
    )
    if not new_s3_files:
        LOGGER.info('No new file found and skipped the task.')
        return
    for object_key_pattern, matching_files_list in new_s3_files.items():
        record_import_timestamp_as_string = get_current_timestamp_as_string()
        sorted_matching_files_list = (
            sorted(matching_files_list,
                   key=lambda file_meta:
                   file_meta[NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY]
                   )
        )

        for matching_file_metadata in sorted_matching_files_list:
            transform_load_data(
                matching_file_metadata.get(
                    NamedLiterals.S3_FILE_METADATA_NAME_KEY
                ),
                data_config,
                record_import_timestamp_as_string,
            )
            updated_obj_pattern_with_latest_dates = (
                update_object_latest_dates(
                    obj_pattern_with_latest_dates,
                    object_key_pattern,
                    matching_file_metadata.get(
                        NamedLiterals.S3_FILE_METADATA_LAST_MODIFIED_KEY
                    )
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


def main():
    parser = argparse.ArgumentParser(description='Run ETL for a specific S3 CSV pipeline')
    parser.add_argument('--data-pipeline-id', required=True)
    args = parser.parse_args()
    data_pipeline_id = args.data_pipeline_id
    LOGGER.info('Starting ETL for pipeline: %s', data_pipeline_id)
    csv_etl(data_pipeline_id)
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
