import argparse
import json
import logging
import os
from typing import Literal, Optional, Sequence

import yaml

from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfigEnvironmentVariables
)
from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesPipelineConfigDict,
    MultiKubernetesPipelineConfigDict
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    update_deployment_env_placeholder
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


LOGGER = logging.getLogger(__name__)


OutputFormatLiteral = Literal['json', 'yaml']
DEFAULT_OUTPUT_FORMAT: OutputFormatLiteral = 'yaml'


def dump_config(config, output_format: OutputFormatLiteral):
    if output_format == 'yaml':
        print(yaml.dump(config))
    elif output_format == 'json':
        print(json.dumps(config))
    else:
        raise AssertionError(f'unrecognised output format: {output_format}')


def get_pipeline_config_dict_by_id(
    multi_pipeline_config_dict: MultiKubernetesPipelineConfigDict,
    data_pipeline_id: str
) -> KubernetesPipelineConfigDict:
    for pipeline_config_dict in multi_pipeline_config_dict['kubernetesPipelines']:
        if pipeline_config_dict['dataPipelineId'] == data_pipeline_id:
            return pipeline_config_dict
    raise KeyError(f'Pipeline with id {data_pipeline_id} not found')


def run(
    config_file_path: str,
    deployment_env: str,
    output_format: OutputFormatLiteral,
    data_pipeline_id: Optional[str] = None
):
    multi_pipeline_config_dict: MultiKubernetesPipelineConfigDict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(config_file_path),
        deployment_env=deployment_env
    )
    if data_pipeline_id:
        dump_config(
            get_pipeline_config_dict_by_id(
                multi_pipeline_config_dict=multi_pipeline_config_dict,
                data_pipeline_id=data_pipeline_id
            ),
            output_format=output_format
        )
    else:
        dump_config(
            multi_pipeline_config_dict,
            output_format=output_format
        )


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
        '--data-pipeline-id',
        type=str
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
        output_format=args.output_format,
        data_pipeline_id=args.data_pipeline_id
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
