# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
from typing import Sequence

from data_pipeline.finance_data.finance_data_pipeline_config import (
    FinanceDataPipelineConfig
)
from data_pipeline.finance_data.finance_data_pipeline import (
    fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class FinanceDataPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'FINANCE_DATA_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'FINANCE_DATA_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'Finance_Data_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config_list() -> Sequence[FinanceDataPipelineConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        FinanceDataPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        FinanceDataPipelineConfig.parse_config_list_from_dict
    )


def fetch_finance_data_and_write_to_s3_task(**_kwargs):
    fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list(
        get_pipeline_config_list()
    )


FINANCE_DATA_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=get_environment_variable_value(
        FinanceDataPipelineEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    FINANCE_DATA_DAG,
    "fetch_finance_data_and_write_to_s3_task",
    fetch_finance_data_and_write_to_s3_task,
    retries=5
)
