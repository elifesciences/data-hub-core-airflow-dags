import logging
import os

from data_pipeline.monitoring.cli import main
from data_pipeline.utils.pipeline_config import PipelineEnvironmentVariables


LOGGER = logging.getLogger(__name__)


ORIGINAL_ENV = os.environ


def test_run_data_hub_pipeline_health_check(
    mock_env: dict
):
    mock_env.update(ORIGINAL_ENV)

    # Hardcoding to `staging` because we do not have `v_Data_Hub_Pipeline_Status`
    #   in the `ci` dataset
    mock_env[
        PipelineEnvironmentVariables.DEPLOYMENT_ENV
    ] = 'staging'
    main()
