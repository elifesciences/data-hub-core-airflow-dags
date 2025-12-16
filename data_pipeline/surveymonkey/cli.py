import argparse
import logging
from typing import Optional, Sequence

from data_pipeline.surveymonkey.get_surveymonkey_data_config import SurveyMonkeyDataConfig
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)


LOGGER = logging.getLogger(__name__)


class SurveyMonkeyPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'SURVEYMONKEY_DATA_CONFIG_FILE_PATH'


def get_pipeline_config() -> 'SurveyMonkeyDataConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        SurveyMonkeyPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        SurveyMonkeyDataConfig.from_dict
    )


def main(argv: Optional[Sequence[str]] = None):
    # Name CLI and declare no arguments
    parser = argparse.ArgumentParser(description="Run ETL for a Survey Monkey pipeline")
    parser.parse_args(argv)

    pipeline_config = get_pipeline_config()
    LOGGER.info('pipeline_config: %r', pipeline_config)

    LOGGER.info('Starting ETL')
    LOGGER.info('ETL process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
