import argparse
import logging
from typing import Optional, Sequence, cast

from data_pipeline.google_spreadsheet.google_spreadsheet_config import (
    MultiCsvSheetConfig,
    MultiSpreadsheetConfig
)
from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    GoogleSpreadsheetConfigDict
)
from data_pipeline.google_spreadsheet.google_spreadsheet_etl import (
    etl_google_spreadsheet
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


class GoogleSpreadsheetEnvironmentVariables:
    CONFIG_FILE_PATH = 'SPREADSHEET_CONFIG_FILE_PATH'


def get_multi_google_spreadsheet_config() -> MultiSpreadsheetConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        GoogleSpreadsheetEnvironmentVariables.CONFIG_FILE_PATH,
        MultiSpreadsheetConfig.from_dict
    )


def google_spreadsheet_etl(data_pipeline_id: str):
    multi_google_spreadsheet_config = get_multi_google_spreadsheet_config()
    data_config_dict = multi_google_spreadsheet_config.spreadsheets_config[data_pipeline_id]
    deployment_env = get_deployment_env()
    data_config = MultiCsvSheetConfig(
        cast(
            GoogleSpreadsheetConfigDict,
            data_config_dict
        ),
        deployment_env=deployment_env
    )
    etl_google_spreadsheet(data_config)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Run ETL for a specific Google Spreadsheet pipeline'
    )
    parser.add_argument('--data-pipeline-id', required=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    data_pipeline_id = args.data_pipeline_id
    LOGGER.info('Starting ETL for pipeline: %s', data_pipeline_id)
    google_spreadsheet_etl(data_pipeline_id)
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
