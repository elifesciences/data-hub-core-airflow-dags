import argparse
import logging
from typing import Optional, Sequence

from data_pipeline.s3_csv_data.s3_csv_config import (
    MultiS3CsvConfig,
    S3BaseCsvConfig
)
from data_pipeline.s3_csv_data.s3_csv_etl import (
    etl_new_csv_files
)
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
