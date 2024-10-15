import argparse
import json
import logging
import os
from typing import Literal, Optional, Sequence

import yaml

from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfigEnvironmentVariables
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    update_deployment_env_placeholder
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


LOGGER = logging.getLogger(__name__)


OutputFormatLiteral = Literal['json', 'yaml']
DEFAULT_OUTPUT_FORMAT: OutputFormatLiteral = 'yaml'


def run(
    config_file_path: str,
    deployment_env: str,
    output_format: OutputFormatLiteral
):
    pipeline_config_dict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(config_file_path),
        deployment_env=deployment_env
    )
    if output_format == 'yaml':
        print(yaml.dump(pipeline_config_dict))
    elif output_format == 'json':
        print(json.dumps(pipeline_config_dict))
    else:
        raise AssertionError(f'unrecognised output format: {output_format}')


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    default_deployment_env = get_deployment_env()
    default_config_file_path = (
        os.getenv(KubernetesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH)
    )
    parser.add_argument(
        '--deployment-env',
        type=str,
        default=default_deployment_env,
        required=not default_deployment_env
    )
    parser.add_argument(
        '--config-file',
        type=str,
        default=default_config_file_path,
        required=not default_config_file_path
    )
    parser.add_argument(
        '--output-format',
        type=str,
        default=DEFAULT_OUTPUT_FORMAT
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    LOGGER.info('Arguments: %r', args)
    run(
        config_file_path=args.config_file,
        deployment_env=args.deployment_env,
        output_format=args.output_format
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
